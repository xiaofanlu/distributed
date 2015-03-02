package tpc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.TreeSet;

import util.Constants;
import util.KVStore;
import util.Message;
import util.PlayList;
import framework.Config;
import framework.NetController;

public class TPCNode implements KVStore {
  private Config config;
  NetController nc;
  private TPCLog log;
  private PlayList pl;
  private boolean isMaster = false;
  private TPCMaster master;
  private TPCSlave slave;
  private Integer viewNum = 0; // Current Master ID
  public TreeSet<Integer> broadcastList;

  public enum SlaveState {
    READY, ABORTED, COMMITTED, COMMITTABLE, UNCERTAIN
  };
  SlaveState state;

  
  public TPCNode(String configFile) {
    try {
      config = new Config(configFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    nc = new NetController(config);
    String logName = "TPCLog" + getProcNum() + ".txt";
    log = new TPCLog(logName, this);
    
    broadcastList = new TreeSet<Integer>();
    for (int i = 0; i < config.numProcesses; i++) {
      if (i != config.procNum) {
        broadcastList.add(i);
      }
    }
    //config.logger.log(Level.WARNING, "Server: Broadcast List");
    
    pl = new PlayList ();
    start();
  }
  
  public void start () {
    if (getProcNum() == 0) {
      System.out.println(">>>> Run as Master");
      isMaster = true;
      master = new TPCMaster(this);
      master.start();
    } else {
      System.out.println(">>>> Run as Slave");
      isMaster = false;
      slave = new TPCSlave(this);
      slave.start();
    }
  }
  
  public int getProcNum() {
    return config.procNum;
  }
  
  public int getMaster() {
    return viewNum;
  }
  
  public void logToScreen(String m) {
    System.out.println("Node " + getProcNum() + ": " + m);
  }

  @Override
  public boolean add(String song, String url) {
    return pl.add(song, url);
  }

  @Override
  public boolean delete(String song, String url) {
    return pl.delete(song, url);
  }

  @Override
  public boolean edit(String song, String url) {
    return pl.edit(song, url);
  }
  
  public void broadcast(Message m) {
    broadcast(m, broadcastList);
  }
  
  public void broadcast(Message m, TreeSet<Integer> list) {
    for (int i : list) {
      unicast(i, m);
    }
  }
  
  public void unicast(int dst, Message m) {
    m.setSrc(getProcNum());
    m.setDst(dst);
    nc.sendMsg(dst, m.marshal());
  }
  
  public int size() {
    return broadcastList.size();
  }
  
  public boolean execute(Message m) {
    if (m.getMessage().equals(Constants.ADD)) {
      return pl.add(m.getSong(), m.getUrl());
    } else if  (m.getMessage().equals(Constants.DEL)) {
      return pl.delete(m.getSong(), m.getUrl());
    } else if  (m.getMessage().equals(Constants.EDIT)) {
      return pl.edit(m.getSong(), m.getUrl());
    }
    return false;
  }
  

  
  
  public void log(Message m) {
    log.appendAndFlush(m);
  }
  
  public class Election extends Thread {
    public void run() {
      logToScreen("Run Election");
    }
  }
  
}
