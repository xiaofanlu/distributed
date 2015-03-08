package tpc;

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
  public Config config;
  NetController nc;
  TPCLog log;
  PlayList pl;
  TPCMaster masterThread;
  TPCSlave slaveThread;
  boolean isRecovery;
  boolean hasRecovered = false;
  //YW: two new parameters
  public boolean isMaster = false;
  public boolean hasMaster; //YW: If a node is recovered, and sees no 
  									//other master reply it, then select itself as new master
  public boolean systemHasMaster; //YW:TRUE: if there is, or will be a master selected
  

  int viewNum; // Current Master ID
  public TreeSet<Integer> broadcastList;
  //Set of Nodes that are operational,  including coordinator and processes
  public UpList upList;

  //private String logName;
  ConcurrentLinkedQueue<Message> messageQueue =
      new ConcurrentLinkedQueue<Message> ();

  // for test
  HashMap<Integer, Integer> messageCount =
      new HashMap<Integer, Integer> ();



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
    // the order of initialization matters!!
    state = SlaveState.ABORTED;
    nc = new NetController(config);
    upList = new UpList(this);
    initBroadcastList();
    viewNum = upList.getMaster();
    pl = new PlayList ();
    new Listener().start();
    //YW: default hasMaster value is true. If the node is recovered, 
    // hasMaster will be set to false in TPCLog and finally set to true after recovery
    hasMaster = true;
    systemHasMaster = true;
    log = new TPCLog("TPCLog" + getProcNum() + ".txt", this);
    start();
  }


  public void initBroadcastList() {
    broadcastList = new TreeSet<Integer>();
    for (int i = 0; i < config.numProcesses; i++) {
      if (i != config.procNum) {
        broadcastList.add(i);
      }
    }
  }


  public void start () {
    if (hasRecovered) {
      return;
    }
    if (getProcNum() == 0 && !isRecovery) {
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
    return (config.get("delay") + 1) * 1000;
  }

  public int getDelayTime() {
    return config.get("delay") * 1000;
  }

  public int getProcNum() {
    return config.procNum;
  }

  public int getMaster() {
    return viewNum;
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
      unicastNow(i, m);
    }
  }
  
  /**
   * YW: Broadcast to ALL nodes at once
   * @param m
   */
  public void broadcastAll(Message m){
	  for(int i = 0;i<config.numProcesses;i++){
		  if(i!= getProcNum()){
			  unicastNow(i, m);
		  }
	  }
  }

  public void unicastNow(int dst, Message m) {
    if (dst < config.numProcesses && dst >= 0) {
      m.setSrc(getProcNum());
      m.setDst(dst);
      nc.sendMsg(dst, m.marshal());
    } else {
      logToScreen("Invaild Destination Number, ignore...");
    }
  }

  public void unicast(int dst, Message m) {
    try {
      Thread.sleep(getDelayTime());
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    unicastNow(dst, m);
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

  public void logToScreen(String m) {
    System.out.println("Node " + getProcNum() + ": " +   m);
  }

  public void logToScreen2(String m) {
    System.out.println("Node " + getProcNum() + m);
  }

  public void printPlayList() {
    pl.print();
  }

  public boolean containsSong(String song) {
    return pl.containsSong(song);
  }

  public void rollback() {
    logToScreen("Rollback last effective command ...");
    Message m = log.getLastVoteReq();
    if (m == null) {
      logToScreen("No such command ... skip ...");
      return;
    }
    m.print();
    if (m.getMessage().equals(Constants.ADD) &&
        containsSong(m.getSong())) {
      logToScreen("Redo add ...");
      delete(m.getSong(), m.getUrl());
    } else if (m.getMessage().equals(Constants.DEL) &&
        !containsSong(m.getSong())) {
      logToScreen("Redo delete ...");
      add(m.getSong(), m.getUrl());
    } else if (m.getMessage().startsWith(Constants.EDIT) && 
        pl.getUrl(m.getSong()).equals(m.getUrl())) {
      logToScreen("Redo edit ...");
      int index = m.getMessage().indexOf('@');
      String oriUrl = m.getMessage().substring(index + 1);
      edit(m.getSong(), oriUrl);
    }
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


  public void runElection() {
    logToScreen(": Running election protocol...");
    Integer temp_viewNum = viewNum;
    if(upList.size() == 1 & viewNum == config.procNum){
      // Node is the only node in the network, If the node itself is always in UpList,
      // then this means node i is already the Master and no other master is available
      logToScreen(": Single Master Left...");
    } else{
      temp_viewNum = (viewNum + 1) % config.numProcesses; // Update viewNum
    }
    //logToScreen("view_num: " + viewNum + "\t temp_viewNum:" + temp_viewNum);
    //temp_viewNum =  upList.upList.ceiling(temp_viewNum);
    // The least element in the UP set that is no smaller than viewNum
    if(temp_viewNum == null){
      logToScreen("Error: Can't find new Master");
    } else {
      upList.remove(viewNum);
      viewNum = temp_viewNum;
      if(temp_viewNum == config.procNum){
        logToScreen("Elected >>> Self <<<  as Master...");
        logToScreen("Invoke coordinator's algorithm of termination protocol...");
        masterThread = new TPCMaster(TPCNode.this, true);
        masterThread.start();
      }
      else{
        logToScreen("Elected >>> " + viewNum +" <<< as Master ...");
        logToScreen("Invoke participant's algorithm of termination protocol...");
        //Message msg = new Message(Constants.UR_SELECTED);
        //unicast(temp_viewNum,msg);
        Message msg = new Message(Constants.UR_SELECTED);
        slaveThread = new TPCSlave(TPCNode.this, true, true);
        slaveThread.start();
        unicast(viewNum, msg);
      }
    }
  }


  /*
   * get stateReq from new master, skip waiting for stateReq
   * run termination immediately
   */
  public void runSlaveTermination () {
    slaveThread = new TPCSlave(TPCNode.this, true, false);
    slaveThread.start();
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


      if (m.isStateQuery()) {
        handleStateQuery(m);
      } else if (m.isStateReply()) {
        handleStateReply(m);
      } else if (m.isJoinReq()) {
        logToScreen("Node " + m.getSrc() + " is back! Welcome!");
        upList.add(m.getSrc()); 
        upList.print();
      } else if (m.isPrintReq()) {
        if (m.getMessage().equals(Constants.PLAYLIST)) {
          printPlayList();
        } else if (m.getMessage().equals(Constants.LOGLIST)) {
          log.printLog();
        }else if (m.getMessage().equals(Constants.UPLIST)) {
          upList.print();
        }
      } else if (m.isKill()) {
        logToScreen("Suicide command received! :(");
        System.exit(-1);
      }
      else {
        messageQueue.offer(m);
        if (!m.isHeartBeat()) {
          messageCount.put(m.getSrc(), 
              messageCount.containsKey(m.getSrc()) ?
                  messageCount.get(m.getSrc()) + 1 : 1);
          if (config.get("deathAfterProcess") == m.getSrc()) {
            if (config.get("deathAfterCount") <=
                messageCount.get(m.getSrc())) {
              logToScreen("Death after count triggered!!");
              logToScreen("From Process: " + m.getSrc());
              logToScreen("Message Count: " + 
                  messageCount.get(m.getSrc()));
              System.exit(-1);
            }
          }
          System.out.println(">>>>>>>>>>>> Message from " + 
              m.getSrc() + ": " + m.getType() + "\t" + m.getMessage());
        }
      }
    }

    public void handleStateQuery(Message m) {
    	//YW: EDIT: First, check whether the node has master
    	if(!hasMaster){
    		if( m.getUrl().equals(Constants.IS_MASTER)){
    			logToScreen("Get query from Master "+m.getSrc()+", has master now");
    			systemHasMaster = true;
    			hasMaster = true;
    			viewNum = m.getSrc();
    			upList.startingNode = viewNum;
    		}
    		else if(m.getUrl().equals(Constants.HAS_MASTER)){
    			systemHasMaster = true;
    		}
    	}
    	
      switch (state) {
      case UNCERTAIN:
      case COMMITTABLE: 
        //logToScreen(": Still conducting tranction, 
        // unable to reply state query ...");
        break;
      case ABORTED:
      case COMMITTED:
        logToScreen("Reply to state query from " + 
            m.getSrc() + ": " + state.name());
        Message stateReply;
        if(isMaster){
        stateReply = 
        		//YW:Changed stateReply Message, add is_master
            new Message(Constants.STATE_REPLY, upList.marshal(), Constants.IS_MASTER, state.name());
        }else if(hasMaster){
        stateReply = 
            		//YW:Changed stateReply Message, add is_master
            new Message(Constants.STATE_REPLY, upList.marshal(), Constants.HAS_MASTER, state.name());
        }
        else{
        	stateReply = 
            		//YW:Changed stateReply Message, add is_master
            new Message(Constants.STATE_REPLY, upList.marshal(), "", state.name());
        }
        //stateReply.print();
        if (!upList.upList.contains(m.getSrc())) {
          upList.add(m.getSrc());
        }
        unicast(m.getSrc(), stateReply);
      }
      if (isRecovery) {
        if (!upList.uLog.containsKey(m.getSrc())) {
          logToScreen("Get StateReq from node " + m.getSrc());
          TreeSet<Integer> otherLog = upList.parseString(m.getMessage());
          upList.uLog.put(m.getSrc(), otherLog);
          logToScreen("Node " + m.getSrc() + " logged upList: " + 
              upList.serialize(otherLog));
          upList.intersection.retainAll(otherLog);
          logToScreen("current intersetion: " + 
              upList.serialize(upList.intersection));
          upList.recoverGroup.add(m.getSrc());
        }
      }

    }

    public void handleStateReply(Message m) {
    	//YW: if the reply stating it is master, then switch master to that node:
    	//YW: EDIT: First, check whether the node has master
    	if(!hasMaster){
    		if( m.getUrl().equals(Constants.IS_MASTER)){
    			logToScreen("Get Reply from Master "+m.getSrc()+", has master now");
    			systemHasMaster = true;
    			hasMaster = true;
    			viewNum = m.getSrc();
    			upList.startingNode = viewNum;
    		}
    		else if(m.getUrl().equals(Constants.HAS_MASTER)){
    			systemHasMaster = true;
    		}
    	}
    	
      switch (state) {
      case UNCERTAIN:
      case COMMITTABLE: 
        logToScreen("Thanks for your state reply! New state: " 
            + m.getMessage());
      case ABORTED:
      case COMMITTED:
        //viewNum = m.getSrc();
        state = SlaveState.valueOf(m.getMessage());
        // update upList from others
        if (m.getSong().length() > 0) {
          upList.updateFromString(m.getSong());
        }
        upList.add(getProcNum());
        //upList.print();      
        break;
      }
    }
  }
}
