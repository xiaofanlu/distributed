package tpc;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


import util.Constants;
import util.Message;


public class TPCSlave extends Thread {
  private TPCNode node;
  private final int TIME_OUT = 6;  // 10 secs timeout
  private Queue<Message> tpcReq = new ConcurrentLinkedQueue<Message> ();
  volatile boolean finished = false;
  boolean stateReq = false;
  boolean terminationResp = false;
  
  private boolean recovery;


  public TPCSlave(TPCNode node) {
    this.node = node;
    node.state = TPCNode.SlaveState.READY;
  }
  
  public TPCSlave(TPCNode node, boolean r) {
    this.node = node;
    recovery = r;
  }

  public void run() {
    new Listener().start();
    if (recovery) {
      runTermination();
    }

    while (!finished) {
      if (tpcReq.isEmpty()) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        Message m = tpcReq.poll();
        runTPC(m);
      }
    }
    if (finished) {
      logToScreen("Slave thread clean shutdown...");
    }
  }

  public void rollback() {
    logToScreen("Receive abort when voted yes... Sad...");
    Message m = node.getPrevMessage();
    if (m.getMessage().equals(Constants.ADD)) {
      node.delete(m.getSong(), m.getUrl());
    } else if (m.getMessage().equals(Constants.DEL)) {
      node.add(m.getSong(), m.getUrl());
    } else if (m.getMessage().startsWith(Constants.EDIT)) {
      int index = m.getMessage().indexOf('@');
      String oriUrl = m.getMessage().substring(index + 1);
      node.edit(m.getSong(), oriUrl);
    }
  }

  public void shutdown () {
    logToScreen("Clean Shutdown command received....");
    logToScreen("Slave thread will shut down soon...");    
    finished = true;
  }

  public void exitAndRunElection() {
    shutdown();
    logToScreen("Shutting down current slave thread...");
    node.new Election().start();
  }

  public void logToScreen(String m) {
    node.logToScreen(m);
  }

  /* 
   * Execute TPC process to respond to 
   * vote request...
   * 
   */
  public void runTPC(Message m) {
    node.state = TPCNode.SlaveState.READY;
    if (executeRequest(m)) {
      getFeedback(TIME_OUT);
      switch (node.state) {
      case UNCERTAIN:
        //TODO: should be a new thread to do that 
        exitAndRunElection();
        return;
      case ABORTED :
        doAbort();
        return;
      case COMMITTABLE:
        logToScreen("Current State: Commitable, continue");
        break;
      default:
        logToScreen("Unexpected State...Stop");
        return;
      }
    }
    phase3();
  }

  public void doCommit() {
    node.log(new Message(Constants.COMMIT));
    node.printPlayList();
  }
  
  public void doAbort() {
    rollback();
    node.log(new Message(Constants.ABORT));
    node.printPlayList();
  }
  
  
  public void phase3 () {
    assert node.state == TPCNode.SlaveState.COMMITTABLE;
    node.unicast(node.getMaster(), new Message(Constants.ACK));
    logToScreen("Reply ACK");
    getCommit(TIME_OUT);
    switch (node.state) {
    case COMMITTABLE:
      exitAndRunElection();
      return;
    case COMMITTED :
      doCommit();
      return;
    default:
      System.out.println("Unexpected State...Stop");
      return;
    }
  }


  /* 
   * Wait for commit from master
   */
  public void getCommit(int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (node.state != TPCNode.SlaveState.COMMITTABLE) {
        return;
      }
      try {
        logToScreen("Waiting for commit from master " + i + " ...");
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return;
  }

  /* 
   * Wait for feedback from master
   */
  public void getFeedback(int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (node.state != TPCNode.SlaveState.UNCERTAIN) {
        return;
      }
      try {
        logToScreen("Waiting for feedback after vote " + i + " ...");
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return;
  }

  /* 
   * Wait for feedback from master
   */
  public void getStateReq(int time_out) {
    for (int i = 0; i < time_out * 2; i++) {
      if (stateReq) {
        return;
      }
      try {
        logToScreen("Waiting for stateReq " + i + " ...");
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return;
  }

  /* 
   * Wait for feedback from master
   */
  public void getResponse(int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (terminationResp) {
        return;
      }
      try {
        logToScreen("Waiting terminationResp" + i);
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return;
  }



  public boolean executeRequest(Message m) {
    assert node.state == TPCNode.SlaveState.READY;
    boolean success = false;
    if (m.getMessage().equals(Constants.ADD)) {
      success = node.add(m.getSong(), m.getUrl());
    } else if (m.getMessage().equals(Constants.DEL)) {
      success = node.delete(m.getSong(), m.getUrl());
    } else if (m.getMessage().startsWith(Constants.EDIT)) {
      success = node.edit(m.getSong(), m.getUrl());
    }
    logToScreen("Get Request: " + m.getMessage() + "\t" + success);
    Message reply = new Message(Constants.RESP, "", "", success? Constants.YES : Constants.NO);
    node.log(m);  // Log the request implies yes
    node.unicast(m.getSrc(), reply);
    logToScreen("Voted: " + reply.getMessage());
    node.state = success ? TPCNode.SlaveState.UNCERTAIN : TPCNode.SlaveState.ABORTED;
    return success;
  }


  /*
   * Participant's algorithm of the 3PC termination protocol
   * paper page 253
   */
  public void runTermination() {
    logToScreen("Start Slave's termination protocol");
    getStateReq(TIME_OUT);
    // timeout, no state request from master
    // run election again...
    if (!stateReq) {  
      exitAndRunElection();
      return;
    }
    reportState(node.getMaster());
    TPCNode.SlaveState pState = node.state;
    getResponse(TIME_OUT);
    if (!terminationResp) {
      exitAndRunElection();
      return;
    }
    if (pState != node.state) {
      switch (node.state) {
      case ABORTED :
        doAbort();
        break;
      case COMMITTED :
        doCommit();
        break;
      case COMMITTABLE:
        phase3();
      default:
        break;
      }
    }
    finishTernimation();
  }

  public void finishTernimation() {
    stateReq = false;
    terminationResp = false;
  }

  public void reportState(int id) {
    logToScreen("Report state ...");
    if (node.state == TPCNode.SlaveState.READY) {
      node.state = TPCNode.SlaveState.ABORTED;
    }
    node.setMaster(id);
    Message stateReport = new Message(Constants.STATE_REP, "", "", node.state.name());
    node.unicast(node.getMaster(), stateReport);
  }


  // inner Listener class
  class Listener extends Thread {
    public void run() {
      while (!finished) {
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
      if (m.isVoteReq()) { 
        tpcReq.offer(m);
      } else if (m.isStateReq()) {
        //logToScreen("Got State Request");
        stateReq = true;
      //  reportState(m.getSrc());
      } else if (m.isFeedback()) {
        processFeedback(m);
      } else if (m.isMaster()) {
        // TODO: update...
        shutdown();
        node.runAsMaster();
      }
    }

    public void processFeedback(Message m) {
      //System.out.println("Get Reply: " + m.getType());
      if (m.getType().equals(Constants.COMMIT)) {
        assert node.state == TPCNode.SlaveState.COMMITTABLE;
        node.state = TPCNode.SlaveState.COMMITTED;
      } else if (m.getType().equals(Constants.ABORT)) {
        node.state = TPCNode.SlaveState.ABORTED;
      } else if (m.getType().equals(Constants.PRECOMMIT)) {
        assert node.state == TPCNode.SlaveState.UNCERTAIN;
        node.state = TPCNode.SlaveState.COMMITTABLE;
      }
      if (stateReq) {
        terminationResp = true;
      }
    }
  }
}

