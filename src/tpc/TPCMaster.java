package tpc;


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
    return twoPC(m);
  }

  @Override
  public boolean delete(String song, String url) {
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.DEL);
    return twoPC(m);
  }

  @Override
  public boolean edit(String song, String url) {
    Message m = new  Message (Constants.VOTE_REQ, song, url, Constants.EDIT);
    return twoPC(m);
  }

  public boolean twoPC(Message m) {
    startTPC(m);
    boolean rst = false;
    if (collectReply(TIME_OUT)) {  // no timeout
      if (noList.size() == 0 && node.execute(m)) {
        doCommit();
        rst = true;
      } else {
        doAbort();
        rst = false;
      }
    } else {  // timeout
      doAbort();
      rst = false;
    }
    finishTPC();
    return rst;
  }
  
  public void doCommit() {
    Message commit = new Message(Constants.COMMIT);
    node.log(commit);
    node.broadcast(commit);
  }
  
  public void doAbort() {
    Message abort = new Message(Constants.ABORT);
    node.log(abort);
    node.broadcast(abort, yesList);
  }


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

  public void startTPC (Message m) {
    yesList = new TreeSet<Integer> ();
    noList  = new TreeSet<Integer> ();
    node.broadcast(m);
    node.log(m);
    logToScreen("Start 2PC " + m.getMessage());
  }

  public void finishTPC() {
    yesList = null;
    noList = null;
    logToScreen("Finish 2PC ");
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
      } 
    }
  }


}