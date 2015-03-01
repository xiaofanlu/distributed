package tpc;

import java.util.List;

import util.Constants;
import util.Message;

import framework.NetController;


public class TPCSlave extends Thread {
  public enum SlaveState {
    READY, ABORTED, COMMITTED, COMMITTABLE, UNCERTAIN
  };


  private TPCNode node;
  private SlaveState state;


  public TPCSlave(TPCNode node) {
    this.node = node;
    state = SlaveState.READY;
    
  }
  
  public void run() {
    new Listener(node.nc).start();
  }
  
  public void rollback() {
    
  }
  
  public void logToScreen(String m) {
    node.logToScreen(m);
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
      if (m.isRequest()) {
        processRequest(m);
      } else if (m.isFeedback()) {
        processFeedback(m);
      }
    }

    public void processFeedback(Message m) {
      System.out.println("Get Reply: " + m.getType());
      if (m.getType().equals(Constants.COMMIT)) {
        assert state == SlaveState.UNCERTAIN;
        state = SlaveState.READY;
      } else if (m.getType().equals(Constants.ABORT)) {
        if (state == SlaveState.ABORTED) {
          state = SlaveState.READY;
        } else if (state == SlaveState.UNCERTAIN) {
          rollback();
        }
      }
    }
    


    public void processRequest(Message m) {
      assert state == SlaveState.ABORTED;
      boolean success = false;
      if (m.getMessage().equals(Constants.ADD)) {
        success = node.add(m.getSong(), m.getUrl());
      } else if (m.getMessage().equals(Constants.DEL)) {
        success = node.delete(m.getSong(), m.getUrl());
      } else if (m.getMessage().equals(Constants.EDIT)) {
        success = node.edit(m.getSong(), m.getUrl());
      }
      logToScreen("Get Request: " + m.getMessage());
      state = success ? SlaveState.UNCERTAIN : SlaveState.ABORTED;
      Message reply = new Message(Constants.RESP, "", "", success ? Constants.YES : Constants.NO);
      logToScreen("Voted: " + reply.getMessage());
      node.unicast(m.getSrc(), reply);
    }

  }

}

