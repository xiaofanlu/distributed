package tpc;


import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.List;

import util.Constants;
import util.KVStore;
import util.Message;
import framework.NetController;

public class TPCMaster extends Thread implements KVStore {
  private TPCNode node;
  public  TreeSet<Integer> yesList = null;
  public TreeSet<Integer> noList = null;
  public TreeSet<Integer> ackList = null;
  public ArrayList<Message> stateReports = null;
  public TreeSet<Integer> uncertainList = null;

  private final int TIME_OUT = 10;  // 10 secs timeout

  public TPCMaster(TPCNode node) {
    this.node = node;
  }

  public void run() {
    new Listener(node.nc).start();
    @SuppressWarnings("resource")
    Scanner sc = new Scanner(System.in);
    while (true) {
      System.out.print("Enter the command, 1 for add, 2 for del, 3 for edit: ");
      int command = sc.nextInt();
      if (command <= 0 || command > 3) {
        System.out.println("Wrong command code!!");
        continue;
      }
      System.out.print("Enter song name: ");
      String song = sc.next();
      System.out.print("Enter song url: ");
      String url = sc.next();
      switch(command) {
      case 1:  add(song, url);
      break;
      case 2:  delete(song, url);
      break;
      case 3:  edit(song, url);
      break;
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public boolean add(String song, String url) {
    logToScreen("ADD " + song + "\t" +  url);
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.ADD);
    return threePC(m);
  }

  @Override
  public boolean delete(String song, String url) {
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.DEL);
    return threePC(m);
  }

  @Override
  public boolean edit(String song, String url) {
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.EDIT);
    return threePC(m);
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
        collectAck(TIME_OUT);
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

  public void doPreCommit(TreeSet<Integer> list) {
    Message precommit = new Message(Constants.PRECOMMIT);
    node.log(precommit);
    node.broadcast(precommit, list);
    logToScreen("Broadcast: precommit");
  }

  public void doCommit() {
    Message commit = new Message(Constants.COMMIT);
    node.log(commit);
    node.broadcast(commit);
    logToScreen("Broadcast: commit");

  }

  public void doAbort(TreeSet<Integer> list) {
    Message abort = new Message(Constants.ABORT);
    node.log(abort);
    node.broadcast(abort, list);
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
        Thread.sleep(1000);
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
  public boolean collectAck(int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (ackList.size() == node.size()) {
        logToScreen("All acks collected.");
        return true;  // no tme_out
      }
      try {
        Thread.sleep(1000);
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



  public void startTPC (Message m) {
    yesList = new TreeSet<Integer> ();
    noList  = new TreeSet<Integer> ();
    ackList = new TreeSet<Integer> ();
    node.broadcast(m);
    node.log(m);
    logToScreen("Start 3PC " + m.getMessage());
  }

  public void finishTPC() {
    yesList = null;
    noList = null;
    ackList = null;
    logToScreen("Finish 3PC ");
  }

  public int getProcNum() {
    return node.getProcNum();
  }

  public void logToScreen(String m) {
    node.logToScreen(m);
  }

  public void broadcast(Message m) {
    node.broadcast(m);
  }

  public void unicast(int dst, Message m) {
    node.unicast(dst, m);
  }
  
  
  /*
   * The termmination protocol on page 252
   * 
   */

  public void termination() {
    stateReports = new ArrayList<Message> ();
    broadcast(new Message(Constants.STATE_REQ));
    collectStateReport(TIME_OUT);
    switch (countStateReport()) {
    case ABORTED : 
      // if the coordinator's DT log does not contain an abort record then 
      doAbort(node.broadcastList);  // ?
      break;
    case COMMITTED : 
      doCommit();
      break;
    case UNCERTAIN :
      if (uncertainList.size() == node.broadcastList.size()) {
        doAbort(node.broadcastList);
      } else {
        doPreCommit(uncertainList);
        // wait for ACK from all participants
        collectAck(TIME_OUT);
        doCommit();
      }
      break;
    default:
      break;
    }
    stateReports = null;
  }


  public void collectStateReport (int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (stateReports.size() == node.size()) {
        return;
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    return;
  }

  public TPCNode.SlaveState countStateReport() {
    for (Message m : stateReports) {
      switch (m.getState()) {
      case ABORTED : 
        return m.getState();
      case COMMITTED : 
        return m.getState();
      case UNCERTAIN : 
        uncertainList.add(m.getSrc());
        break;
      default:
        break;
      }
    }
    return TPCNode.SlaveState.UNCERTAIN;
  }





  // inner Listener class
  class Listener extends Thread {
    NetController nc;
    public Listener (NetController nc) {
      this.nc = nc;
    }

    public void run() {
      while (true) {
        List<String> buffer = nc.getReceivedMsgs();
        for (String str : buffer) {
          processMessage(str);
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public void processMessage(String str) {
      Message m = new Message ();
      m.unmarshal(str);
      if (m.isResponse()) {
        if (m.getMessage().equals(Constants.YES) && yesList != null) {
          yesList.add(m.getSrc());
        } else if (m.getMessage().equals(Constants.NO) && noList != null) {
          noList.add(m.getSrc());
        }
      } else if (m.isAck() && ackList != null) {
        ackList.add(m.getSrc());
      }
    }
  }


}