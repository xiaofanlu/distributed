package tpc;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


import util.Constants;
import util.Message;
import framework.NetController;


public class TPCSlave extends Thread {
  private TPCNode node;
  private final int TIME_OUT = 10;  // 10 secs timeout
  private Queue<Message> tpcReq;
  volatile boolean finished = false;
  boolean stateReq = false;
  boolean terminationResp = false;


  public TPCSlave(TPCNode node) {
    this.node = node;
    node.state = TPCNode.SlaveState.READY;
    tpcReq = new ConcurrentLinkedQueue<Message> ();
  }

  public void run() {
    new Listener(node.nc).start();
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
  }

  public void rollback() {

  }
  
  public void shutdown () {
    finished = true;
  }
  
  public void exitAndRunElection() {
    shutdown();
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
        rollback();
        node.log(new Message(Constants.ABORT));
        return;
      case COMMITTABLE:
        System.out.println("Commitable, continue");
        break;
      default:
        System.out.println("Unexpected State...Stop");
        return;
      }
    }
    phase3();
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
      node.log(new Message(Constants.COMMIT));
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
        Thread.sleep(1000);
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
        logToScreen("Waiting " + i);
        Thread.sleep(1000);
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
    for (int i = 0; i < time_out; i++) {
      if (stateReq) {
        return;
      }
      try {
        logToScreen("Waiting StateReq" + i);
        Thread.sleep(1000);
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
        Thread.sleep(1000);
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
    } else if (m.getMessage().equals(Constants.EDIT)) {
      success = node.edit(m.getSong(), m.getUrl());
    }
    logToScreen("Get Request: " + m.getMessage() + "\t" + success);

    Message reply = new Message(Constants.RESP, "", "", success? Constants.YES : Constants.NO);
    logToScreen("Voted: " + reply.getMessage());
    node.log(reply);
    node.unicast(m.getSrc(), reply);
    node.state = success ? TPCNode.SlaveState.UNCERTAIN : TPCNode.SlaveState.ABORTED;
    return success;
  }
  
  
  /*
   * Participant's algorithm of the 3PC termination protocol
   * paper page 253
   */
  public void termination() {
    getStateReq(TIME_OUT);
    // timeout, no state request from master
    // run election again...
    if (!stateReq) {  
      exitAndRunElection();
    }
    reportState();
    TPCNode.SlaveState pState = node.state;
    getResponse(TIME_OUT);
    if (!terminationResp) {
      exitAndRunElection();
    }
    if (pState != node.state) {
      switch (node.state) {
      case ABORTED :
        rollback();
        node.log(new Message(Constants.ABORT));
        break;
      case COMMITTED :
        node.log(new Message(Constants.COMMIT));
        break;
      case COMMITTABLE:
        phase3();
      }
    }
    finishTernimation();
  }
  
  public void finishTernimation() {
    stateReq = false;
    terminationResp = false;
  }
  
  public void reportState() {
    if (node.state == TPCNode.SlaveState.READY) {
      node.state = TPCNode.SlaveState.ABORTED;
    }
    Message stateReport = new Message(Constants.STATE_REP, "", "", node.state.name());
    node.unicast(node.getMaster(), stateReport);
  }
  

  // inner Listener class
  class Listener extends Thread {
    NetController nc;
    public Listener (NetController nc) {
      this.nc = nc;
    }

    public void run() {
      while (!finished) {
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
      if (m.isVoteReq()) {  // vote_req
        //runTPC(m);
        tpcReq.offer(m);
      } else if (m.isStateReq()) {
        stateReq = true;
      } else if (m.isFeedback()) {
        processFeedback(m);
      }
    }

    public void processFeedback(Message m) {
      System.out.println("Get Reply: " + m.getType());
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

