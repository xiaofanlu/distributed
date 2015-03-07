package tpc;


import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeSet;

import util.Constants;
import util.KVStore;
import util.Message;

public class TPCMaster extends Thread implements KVStore {
  private TPCNode node;
  public  TreeSet<Integer> yesList = null;
  public TreeSet<Integer> noList = null;
  public TreeSet<Integer> ackList = null;
  public ArrayList<Message> stateReports = null;
  public TreeSet<Integer> uncertainList = null;
  private boolean recovery = false;

  private final int TIME_OUT = 4;  // 10 secs timeout

  public TPCMaster(TPCNode node) {
    this.node = node;
    node.state = TPCNode.SlaveState.ABORTED;
    // just to create an empty log
    node.log(new Message(Constants.ABORT));
  }

  public TPCMaster(TPCNode node, boolean r) {
    this.node = node;
    recovery = r;
  }

  public void run() {
    new Listener().start();
    new HeartBeater().start();

    if (recovery) {
      runTermination();
    }
    Scanner sc = new Scanner(System.in);
    while (true) {
      int command = getIntInput(sc, "Enter num for command (" + 
          "1: add, 2: del, 3: edit, 4: playList, 5: log, 6: upList) : ");
      if (command <= 0 || command > 6) {
        System.out.println("Invalid command code!!");
        continue;
      }
      if (command <= 3 ) {
        System.out.print("Enter song name: ");
        String song = sc.nextLine();
        System.out.print("Enter song url: ");
        String url = sc.nextLine();
        switch(command) {
        case 1:  add(song, url);
        break;
        case 2:  delete(song, url);
        break;
        case 3:  edit(song, url);
        break;
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        int id = getIntInput(sc, "Enter the node id: ");
        if (command == 4) {
          node.unicastNow(id, new Message(Constants.PRINT, "", "", 
              Constants.PLAYLIST));
          //node.printPlayList();
        } else if (command == 5){
          node.unicastNow(id, new Message(Constants.PRINT, "", "", 
              Constants.LOGLIST));
        } else {
          node.unicastNow(id, new Message(Constants.PRINT, "", "", 
              Constants.UPLIST));
        }

      }
    }
  }

  public int getIntInput(Scanner sc, String prop) {
    System.out.print(prop);
    while (!sc.hasNextInt()) {
      sc.nextLine();
      System.out.print(prop);  
    }
    int num = sc.nextInt();
    sc.nextLine();
    return num;
  }

  @Override
  public boolean add(String song, String url) {
    logToScreen("Add " + song + "\t" +  url);
    if (node.containsSong(song)) {
      logToScreen("Invaid Command, " + song + " is already there");
      logToScreen("Consider Edit instead :)");
      return false;
    }
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.ADD);
    return threePC(m);
  }

  @Override
  public boolean delete(String song, String url) {
    logToScreen("Delte " + song + "\t" +  url);
    if (!node.containsSong(song)) {
      logToScreen("Invaid Command, " + song + " doesn't exist");
      return false;
    }
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.DEL);
    return threePC(m);
  }

  @Override
  public boolean edit(String song, String url) {
    logToScreen("Edit " + song + "\t" +  url);
    if (!node.containsSong(song)) {
      logToScreen("Invaid Command, " + song + " doesn't exist");
      return false;
    }
    String oriUrl = node.pl.getUrl(song);
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.EDIT + "@" + oriUrl);
    return threePC(m);
  }


  public int getProcNum() {
    return node.getProcNum();
  }

  public void logToScreen(String m) {
    node.logToScreen2("M: " + m);
  }

  public void broadcast(Message m) {
    node.broadcast(m);
  }

  public void unicast(int dst, Message m) {
    node.unicast(dst, m);
  }


  public void doPreCommit(TreeSet<Integer> list) {
    Message precommit = new Message(Constants.PRECOMMIT);
    if (node.state != TPCNode.SlaveState.COMMITTABLE) {
      node.log(precommit);
      node.state = TPCNode.SlaveState.COMMITTABLE;
    }
    int ppc = node.config.get("partialPreCommit");
    if (ppc >= 0) {
      logToScreen("Partial preCommit to " + ppc);
      node.unicast(ppc, precommit);
      logToScreen("Oh no, I am crashing...");
      System.exit(-1);
    } else {
      node.broadcast(precommit, list);
      logToScreen("Broadcast: precommit");
    }
  }

  public void doCommit() {
    Message commit = new Message(Constants.COMMIT);
    node.log(commit);
    node.state = TPCNode.SlaveState.COMMITTED;
    int pc = node.config.get("partialCommit");
    if (pc >= 0) {
      logToScreen("Partial commit to " + pc);
      node.unicast(pc, commit);
      logToScreen("Oh no, I am crashing...");
      System.exit(-1);
    } else {
      logToScreen("Broadcast: commit");
      node.broadcast(commit);
    }
  }

  public void doAbort(TreeSet<Integer> list) {
    node.rollback();
    Message abort = new Message(Constants.ABORT);
    node.log(abort);
    node.state = TPCNode.SlaveState.ABORTED;
    node.broadcast(abort, list);
  }

  public boolean twoPC(Message m) {
    startTPC(m);
    boolean rst = false;
    if (collectReply(TIME_OUT)) {  // no timeout
      if (noList.size() == 0 && node.execute(m)) {
        doCommit();
        rst = true;
      } else {
        doAbort(node.broadcastList);
        rst = false;
      }
    } else {  // timeout
      doAbort(node.broadcastList);
      rst = false;
    }
    finishTPC();
    return rst;
  }


  public void startTPC (Message m) {
    yesList = new TreeSet<Integer> ();
    noList  = new TreeSet<Integer> ();
    ackList = new TreeSet<Integer> ();
    node.broadcast(m);
    node.log(m);
    node.state = TPCNode.SlaveState.UNCERTAIN;
    logToScreen("Start 3PC " + m.getMessage());
  }

  public void finishTPC() {
    yesList = null;
    noList = null;
    ackList = null;
    logToScreen("Finish 3PC ");
    node.printPlayList();
  }


  /*
   * wait for vote message from all participants
   *  on timeout abort
   */
  public boolean collectReply(int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (yesList.size() + noList.size() == node.size()) {
        logToScreen("All votes collected. Yes: " + yesList.size() + " No: " + noList.size());
        return true;  // no tme_out
      }
      try {
        logToScreen("Waiting for vote relay " + i + "...");
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    logToScreen("Timeout");
    return false; // timeout
  }


  /*
   * wait for ACK from all participants
   *  on timeout skip, ingore participant failures
   */
  public boolean collectAck(int time_out, int size) {
    for (int i = 0; i < time_out; i++) {
      if (ackList.size() == size) {
        logToScreen("All acks collected.");
        return true;  // no tme_out
      }
      try {
        logToScreen("Waiting for ack relay " + i + "...");
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    // update alive list?
    logToScreen("Some acks missing");
    for (int i : ackList) {
      logToScreen("Ack from Node " + i);
    }
    return false; // timeout
  }


  /*
   * Coordinator's algorithm for 3PC in p. 250
   */
  public boolean threePC(Message m) {
    startTPC(m);
    boolean rst = false;
    if (!collectReply(TIME_OUT)) {    //no timeout
      doAbort(yesList);
    } else {
      // if all message were YES and coordinator voted Yes
      if (noList.size() == 0 && node.execute(m)) {  
        // send pre-commit to all participants
        doPreCommit(node.broadcastList);
        // wait for ACK from all participants
        collectAck(TIME_OUT, node.size());
        doCommit();
        rst = true;
      } 
      // if some process voted No
      else {
        doAbort(yesList);
      } 
    }  
    finishTPC();
    return rst;
  }



  public void collectStateReport (int time_out) {
    node.broadcast(
        new Message(Constants.STATE_REQ, "", "", node.upList.marshal()), 
        node.upList.getBroadcastList());

    for (int i = 0; i < time_out; i++) {
      if (stateReports.size() == node.size()) {
        return;
      } else {
        try {
          logToScreen("Waiting for stateReport...");
          for (Message m : stateReports) {
            logToScreen("   Have report from: " + m.getSrc());
          }
          Thread.sleep(node.getSleepTime());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    return;
  }

  public TPCNode.SlaveState countStateReport() {
    logToScreen("Counting state reports, I have " + stateReports.size());
    int committableCount = 0;
    Message masterState = new Message(Constants.STATE_REP, "", "", node.state.name());
    masterState.setSrc(node.getProcNum());
    stateReports.add(masterState);

    for (Message m : stateReports) {
      switch (m.getState()) {
      case ABORTED : 
        return m.getState();
      case COMMITTED : 
        return m.getState();
      case UNCERTAIN : 
        uncertainList.add(m.getSrc());
        break;
      case COMMITTABLE :
        committableCount++;
      default:
        break;
      }
    }
    if (uncertainList.size() > node.size()) {
      return TPCNode.SlaveState.UNCERTAIN;
    } else if (committableCount > 0) {
      return TPCNode.SlaveState.COMMITTABLE;
    } else {
      logToScreen("!!!!!!!!!!!!!!!!!!!!!!!!!!! Unexpected state..");
      return TPCNode.SlaveState.ABORTED;
    }

  }

  public void startTermination() {
    logToScreen("Start Master's termination protocol");
    stateReports = new ArrayList<Message> ();
    uncertainList = new TreeSet<Integer>();
    ackList = new TreeSet<Integer> ();
    // wait for all thread to get ready
    try {
      Thread.sleep(node.getSleepTime() * 2);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void finishTermination() {
    logToScreen("Finish Master's termination protocol");
    stateReports = null;
    uncertainList = null;
    ackList = null;
    node.printPlayList();
    //
  }



  /*
   * The termmination protocol on page 252
   * 
   */
  public void runTermination() {
    startTermination();
    //   broadcast(new Message(Constants.STATE_REQ));
    collectStateReport(TIME_OUT);
    switch (countStateReport()) {
    case ABORTED : 
    case UNCERTAIN:
      // if the coordinator's DT log does not contain an abort record then 
      doAbort(node.broadcastList);  // ?
      break;
    case COMMITTED : 
      doCommit();
      break;
    case COMMITTABLE :
      doPreCommit(uncertainList);
      // wait for ACK from all uncertain participants ...
      collectAck(TIME_OUT, uncertainList.size());
      doCommit();
      break;
    default:
      break;
    }
    finishTermination();
  }

  // inner Listener class
  class Listener extends Thread {
    public void run() {
      while (true) {
        if (!node.messageQueue.isEmpty()) {
          processMessage(node.messageQueue.poll());
        } else {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }

    public void processMessage(Message m) {
      if (m.isResponse()) {
        if (m.getMessage().equals(Constants.YES) && yesList != null) {
          yesList.add(m.getSrc());
        } else if (m.getMessage().equals(Constants.NO) && noList != null) {
          noList.add(m.getSrc());
        }
      } else if (m.isAck() && ackList != null) {
        ackList.add(m.getSrc());
      } else if (m.isStateReport() && stateReports != null) {
        stateReports.add(m);
      }
    }
  }

  // inner Listener class
  class HeartBeater extends Thread {
    public void run() {
      while (true) {
        node.broadcast(new Message(Constants.HEART_BEAT));
        try {
          Thread.sleep(node.getDelayTime());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }


}