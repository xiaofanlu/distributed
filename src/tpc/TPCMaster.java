package tpc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.List;
import java.util.logging.Level;

import util.Constants;
import util.KVStore;
import util.Message;
import util.PlayList;
import framework.Config;
import framework.NetController;

/*
 * For test only
 * 
 */

public class TPCMaster implements KVStore {
  private Config config;
  private NetController nc;
  private TPCLog log;
  private PlayList pl;
 
  public TreeSet<Integer> broadcastList;

  public TPCMaster(String configFile) {
    try {
      config = new Config(configFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    config.logger.log(Level.WARNING, "Server: Started");
    nc = new NetController(config);

    broadcastList = new TreeSet<Integer>();
    for (int i = 0; i < config.numProcesses; i++) {
      if (i != config.procNum) {
        broadcastList.add(i);
      }
    }
    config.logger.log(Level.WARNING, "Server: Broadcast List");
    
    pl = new PlayList ();
  }
  
  @Override
  public boolean add(String song, String url) {
    config.logger.log(Level.WARNING, "ADD " + song + "\t" +  url);
    logToScreen("ADD " + song + "\t" +  url);
    Message m = new  Message (Constants.ADD_REQ, song, url);
    if (twoPC(m)) {
      return pl.add(song, url);
    } else {
      return false;
    }
  }

  @Override
  public boolean delete(String song, String url) {
    Message m = new  Message (Constants.DEL_REQ, song, url);
    if (twoPC(m)) {
      return pl.delete(song, url); 
    } else {
      return false;
    }
  }

  @Override
  public boolean edit(String song, String url) {
    Message m = new  Message (Constants.EDIT_REQ, song, url);
    if (twoPC(m)) {
      return pl.edit(song, url); 
    } else {
      return false;
    }
  }

  public boolean twoPC(Message m) {
    broadcast(m);
    logToScreen("Broadcast Request " + m.getType());
    boolean commit = getReply();
    Message reply = new Message(commit ? Constants.COMMIT : Constants.ABORT);
    broadcast(reply);
    logToScreen("Broadcast Reply: " + reply.getType());
    return commit;
  }
  
  public void broadcast(Message m) {
    for (int i : broadcastList) {
      unicast(i, m);
    }
  }
  
  public void unicast(int dst, Message m) {
    m.setSrc(getProcNum());
    m.setDst(dst);
    nc.sendMsg(dst, m.marshal());
  }
  
  
  public boolean getReply() {
    HashSet<Integer> replied = new HashSet<Integer> ();
    boolean commit = true;
    while (replied.size() < broadcastList.size()) {
      List<String> buffer = nc.getReceivedMsgs();
      for (String str : buffer) {
        Message m = new Message ();
        m.unmarshal(str);
        logToScreen("From: " + m.getSrc() + "\t Type" + m.getType());
        if (m.getType().equals(Constants.NO)) {
          commit = false;
        } 
        replied.add(m.getSrc());
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return commit;
  }
  
  public int getProcNum() {
    return config.procNum;
  }
  
  public void logToScreen(String m) {
    System.out.println("Server: " + m);
  }

}