package tpc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import util.Constants;
import util.Message;
import util.PlayList;
import framework.Config;
import framework.NetController;

/*
 * For test only
 * 
 */
public class TPCSlave {
  private Config config;
  private NetController nc;
  private TPCLog log;
  private PlayList pl;
  
  private enum State {
    READY, ABORTED, COMMITTED, COMMITTABLE, UNCERTAIN
  };
  
  private State state;
  

  public TPCSlave(String configFile) {
    try {
      config = new Config(configFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    nc = new NetController(config);
    pl = new PlayList ();
    state = State.READY;
  }

  public void start() {
    while (true) {
      List<String> buffer = nc.getReceivedMsgs();
      if (!buffer.isEmpty()) {
        for (String str : buffer) {
          
          Message m = new Message ();
          m.unmarshal(str);
          processMessage(m);
           
        }
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  public void processMessage(Message m) {
    if (m.isRequest()) {
      processRequest(m);
    } else if (m.isFeedback()) {
      processFeedback(m);
    }
  }
  
  public void processFeedback(Message m) {
    System.out.println("Get Reply: " + m.getType());
    if (m.getType().equals(Constants.COMMIT)) {
      assert state == State.UNCERTAIN;
      state = State.READY;
    } else if (m.getType().equals(Constants.ABORT)) {
      if (state == State.ABORTED) {
        state = State.READY;
      } else if (state == State.UNCERTAIN) {
        rollback();
      }
    }
  }
  
  public void rollback() {
    // lalalla
  }
  
  
  public void processRequest(Message m) {
    assert state == State.ABORTED;
    boolean success = false;
    if (m.getType().equals(Constants.ADD_REQ)) {
      success = pl.add(m.getSong(), m.getUrl());
    } else if (m.getType().equals(Constants.DEL_REQ)) {
      success = pl.delete(m.getSong(), m.getUrl());
    } else if (m.getType().equals(Constants.EDIT_REQ)) {
      success = pl.edit(m.getSong(), m.getUrl());
    }
    logToScreen("Get Request: " + m.getType());
    state = success ? State.UNCERTAIN : State.ABORTED;
    Message reply = new Message(success ? Constants.YES : Constants.NO);
    logToScreen("Voted: " + reply.getType());
    unicast(m.getSrc(), reply);
  }
  
  public void unicast(int dst, Message m) {
    m.setSrc(getProcNum());
    m.setDst(dst);
    nc.sendMsg(dst, m.marshal());
  }
  
  public int getProcNum() {
    return config.procNum;
  }
  
  public void logToScreen(String m) {
    System.out.println("Slave: " + m);
  }
  
}

