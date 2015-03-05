package tpc;

import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import util.Constants;
import util.Message;



public class TPCSlave extends Thread {
  private TPCNode node;
  private final int TIME_OUT = 6;  // 10 secs timeout
  private Queue<Message> tpcReq = new ConcurrentLinkedQueue<Message> ();
  volatile boolean finished = false;
  boolean stateReq = false;
  boolean expectStateReq = false;
  boolean terminationResp = false;
  HeartBeatTimer hbt = new HeartBeatTimer();

  private boolean recovery;


  public TPCSlave(TPCNode node) {
    this.node = node;
    node.state = TPCNode.SlaveState.ABORTED;
    // just to create an empty log
    node.log(new Message(Constants.ABORT));
  }

  public TPCSlave(TPCNode node, boolean r) {
    this.node = node;
    recovery = r;
  }

  public void run() {
    new Listener().start();
    hbt.start();

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


  public void shutdown () {
    logToScreen("Clean Shutdown command received....");
    logToScreen("Slave thread will shut down soon...");    
    finished = true;
    hbt.cancel();
  }

  public void exitAndRunElection() {
    shutdown();
    logToScreen("Shutting down current slave thread...");
    node.new Election().start();
  }

  public void logToScreen(String m) {
    node.logToScreen2("S: " + m);
  }

  /* 
   * Execute TPC process to respond to 
   * vote request...
   * 
   */
  public void runTPC(Message m) {
    node.state = TPCNode.SlaveState.ABORTED;
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
    if (!terminationResp || !node.log.getLastEntry().isCommit()) {
      node.log(new Message(Constants.COMMIT));
    }
    node.printPlayList();
  }

  public void doAbort() {
    node.rollback();
    if (!terminationResp || !node.log.getLastEntry().isAbort()) {
      node.log(new Message(Constants.ABORT));
    }
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
        hbt.setTimeout((time_out + 1) *  node.getSleepTime());
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
        hbt.setTimeout((time_out + 1) *  node.getSleepTime());
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
    expectStateReq = true;
    for (int i = 0; i < time_out * 2; i++) {
      if (stateReq) {
        return;
      }
      try {
        logToScreen("Waiting for stateReq " + i + " ...");
        hbt.setTimeout((time_out * 2 + 1) * node.getSleepTime());
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return;
  }

  /* 
   * Wait for response from master in the termination protocol
   */
  public void getResponse(int time_out) {
    for (int i = 0; i < time_out; i++) {
      if (terminationResp) {
        return;
      }
      try {
        logToScreen("Waiting for termination response " + i);
        hbt.setTimeout((time_out + 1) *  node.getSleepTime());
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return;
  }



  public boolean executeRequest(Message m) {
    assert node.state == TPCNode.SlaveState.ABORTED;
    boolean success = false;
    node.log(m);  // log the vote_req 
    if (m.getMessage().equals(Constants.ADD)) {
      success = node.add(m.getSong(), m.getUrl());
    } else if (m.getMessage().equals(Constants.DEL)) {
      success = node.delete(m.getSong(), m.getUrl());
    } else if (m.getMessage().startsWith(Constants.EDIT)) {
      success = node.edit(m.getSong(), m.getUrl());
    }
    logToScreen("Get Request: " + m.getMessage() + "\t" + success);
    Message reply = new Message(Constants.RESP, "", "", success? Constants.YES : Constants.NO);
    node.log(reply); // log reply
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
    //reportState(node.getMaster());
    TPCNode.SlaveState pState = node.state;
    if (node.state == TPCNode.SlaveState.UNCERTAIN) {
      getResponse(TIME_OUT);
    } else {
      // wait longer for the uncertain node to switch ...
      getResponse(2 * TIME_OUT);
    }
    // response from master in the termination protocol
    if (!terminationResp) {
      exitAndRunElection();
      return;
    }
    /* if state change */
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
    } else {
      switch (node.state) {
      case ABORTED :
      case COMMITTED :
        node.printPlayList();
        break;
      default:
        logToScreen(">>>>>>>>>>>>> Warning: Unexpected state ...");
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
      } else if (m.isStateReq() && expectStateReq) {
        //logToScreen("Got State Request");
        stateReq = true;
        expectStateReq = false;
        reportState(m.getSrc());
      } else if (m.isFeedback()) {
        processFeedback(m);
      } else if (m.isMaster()) {
        // TODO: update...
        if (!finished) {
          shutdown();
          node.runAsMaster();
        }
      } else if (m.isHeartBeat()) {
        if (m.getSrc() != node.getMaster()) {
          logToScreen("Heart beat not from current view num...");
          logToScreen("Update current master to >>> " + m.getSrc() + "<<<");
          if (node.log.recovery) {
            node.viewNum = m.getSrc();
          }
        }
        hbt.reset();
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
        node.log(m);  // log precommit
      }
      if (stateReq) {
        terminationResp = true;
      }
    }
  }


  public Timer startTiming() {
    Timer timer = new Timer();
    timer.schedule(new TimeoutTask(), TIME_OUT * node.getDelayTime() );
    return timer;
  }

  public class HeartBeatTimer extends Thread{
    public TimeoutTask task;
    public Timer timer;


    public HeartBeatTimer(){
      timer = new Timer();
      task = new TimeoutTask();
    }

    public void cancel () {
      task.cancel();
    }

    public void reset() {
      setTimeout(node.getDelayTime() * 9);
    }

    @Override
    public void run(){
      setTimeout(node.getDelayTime() * 20);
    }

    public void setTimeout(long delay){
      task.cancel();
      task = new TimeoutTask();
      timer.schedule(task, delay);
    }
  }

  public class TimeoutTask extends TimerTask{

    /*
     * (non-Javadoc)
     * @see java.util.TimerTask#run()
     * Failed to receive heart-beat within some time. 
     * Election and termination. 
     */
    @Override
    public void run(){
      logToScreen(" >>> time out!!! Coordinator must be down...");
      exitAndRunElection();
    }
  }
}

