package tpc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import util.Constants;
import util.KVStore;
import util.Message;
import util.PlayList;
import util.UpList;
import framework.Config;
import framework.NetController;

public class TPCNode implements KVStore {
  Config config;
  NetController nc;
  private TPCLog log;
  PlayList pl;
  private TPCMaster masterThread;
  private TPCSlave slaveThread;

  int viewNum; // Current Master ID
  public TreeSet<Integer> broadcastList;
  //Set of Nodes that are operational,  including coordinator and processes
  public UpList upList;
  
  private String logName;
  private String upListName;
  private boolean recovery;

  ConcurrentLinkedQueue<Message> messageQueue = new ConcurrentLinkedQueue<Message> ();


  // for test
  HashMap<Integer, Integer> messageCount = new HashMap<Integer, Integer> ();



  public enum SlaveState {
    ABORTED, COMMITTED, COMMITTABLE, UNCERTAIN
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

    logName = "TPCLog" + getProcNum() + ".txt";
    File logFile = new File(logName);
    // log found, recover from log
    if(logFile.exists() && !logFile.isDirectory()) {
      recovery = true;
      log = new TPCLog(logName, this);
      log.rebuildServer();
    } else {
      recovery = false;
      log = new TPCLog(logName, this);
    }
    
    upListName = "TPCUpList" + getProcNum() + ".txt";
    
    broadcastList = new TreeSet<Integer>();
    
    upList = new UpList(config.numProcesses);
    for (int i = 0; i < config.numProcesses; i++) {
      if (i != config.procNum) {
        broadcastList.add(i);
      }
      upList.add(i);
    }
    
    viewNum = upList.getMaster();
    //config.logger.log(Level.WARNING, "Server: Broadcast List");

    pl = new PlayList ();
    start();
  }

  public void start () {
    new Listener().start();  
    if (getProcNum() == 0) {
      System.out.println(">>>> Run as Master");
      masterThread = new TPCMaster(this);
      masterThread.start();
    } else {
      System.out.println(">>>> Run as Slave");
      slaveThread = new TPCSlave(this);
      slaveThread.start();
    }
  }

  public int getSleepTime() {
    return (config.delay + 1) * 1000;
  }

  public int getDelayTime() {
    return config.delay * 1000;
  }

  public int getProcNum() {
    return config.procNum;
  }

  public int getMaster() {
    return viewNum;
  }

  public void logToScreen(String m) {
    System.out.println("Node " + getProcNum() +  m);
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
    try {
      Thread.sleep(getDelayTime());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (int i : list) {
      unicastNoDealy(i, m);
    }
  }

  public void unicastNoDealy(int dst, Message m) {
    m.setSrc(getProcNum());
    m.setDst(dst);
    nc.sendMsg(dst, m.marshal());
  }

  public void unicast(int dst, Message m) {
    try {
      Thread.sleep(getDelayTime());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    m.setSrc(getProcNum());
    m.setDst(dst);
    nc.sendMsg(dst, m.marshal());
  }

  public int size() {
    return upList.size();
  }

  public boolean execute(Message m) {
    if (m.getMessage().equals(Constants.ADD)) {
      return pl.add(m.getSong(), m.getUrl());
    } else if  (m.getMessage().equals(Constants.DEL)) {
      return pl.delete(m.getSong(), m.getUrl());
    } else if  (m.getMessage().startsWith(Constants.EDIT)) {
      return pl.edit(m.getSong(), m.getUrl());
    }
    return false;
  }

  public void log(Message m) {
    log.appendAndFlush(m);
  }

  public void printPlayList() {
    pl.print();
  }

  public Message getPrevMessage () {
    return log.getLastEntry();
  }

  public boolean containsSong(String song) {
    return pl.containsSong(song);
  }
  
  public void setMaster(int id) {
    viewNum = id;
    upList.setMaster(id);
  }
  
  public void runAsMaster() {
    setMaster(getProcNum());
    masterThread = new TPCMaster(TPCNode.this, true);
    masterThread.start();
  }
  

  public class Election extends Thread {
    public void run() {
      logToScreen("Running election protocol...");
      Integer temp_viewNum = viewNum;
      if(upList.size() == 1 & viewNum == config.procNum){
        // Node is the only node in the network, If the node itself is always in UpList,
        // then this means node i is already the Master and no other master is available
        logToScreen("Single Master Left...");
      } else{
        temp_viewNum = (viewNum + 1) % config.numProcesses; // Update viewNum
      }
      //logToScreen("view_num: " + viewNum + "\t temp_viewNum:" + temp_viewNum);
      //temp_viewNum =  upList.upList.ceiling(temp_viewNum);// The least element in the UP set that is no smaller than viewNum
      if(temp_viewNum == null){
        logToScreen("Can't find new Master, error");
      } else {
        upList.remove(viewNum);
        viewNum = temp_viewNum;
        if(temp_viewNum == config.procNum){
          logToScreen("Self selected as Master...");
          logToScreen("Invoke coordinator's algorithm of termination protocol...");
          masterThread = new TPCMaster(TPCNode.this, true);
          masterThread.start();
        }
        else{
          logToScreen("Select >>> " + viewNum +" <<< as Master ...");
          logToScreen("Invoke participant's algorithm of termination protocol...");
          //Message msg = new Message(Constants.UR_SELECTED);
          //unicast(temp_viewNum,msg);
          Message msg = new Message(Constants.UR_SELECTED);
          slaveThread = new TPCSlave(TPCNode.this, true);
          slaveThread.start();
          unicast(viewNum, msg);
        }
      }
    }
  }

  // inner Listener class
  class Listener extends Thread {
    public void run() {
      while (true) {
        List<String> buffer = nc.getReceivedMsgs();
        for (String str : buffer) {
          processMessage(str);
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    public void processMessage(String str) {
      Message m = new Message ();
      m.unmarshal(str);
      messageQueue.offer(m);
      messageCount.put(m.getSrc(), messageCount.containsKey(m.getSrc()) ?
          messageCount.get(m.getSrc()) + 1 : 1);
      if (config.deathAfterProcess == m.getSrc()) {
        if (config.deathAfterCount <= messageCount.get(m.getSrc())) {
          logToScreen("Death after count triggered!!");
          logToScreen("From Process: " + m.getSrc());
          logToScreen("Message Count: " + messageCount.get(m.getSrc()));
          System.exit(-1);
        }
      }
      System.out.println(">>>>>>>>>>>> Message from " + m.getSrc() + ": " + m.getType() + "\t" + m.getMessage());
    }
  }

}
