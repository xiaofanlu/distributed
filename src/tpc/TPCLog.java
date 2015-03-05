package tpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import util.Constants;
import util.Message;

public class TPCLog {

  private String logPath;
  private TPCNode node;
  private ArrayList<Message> entries;
  boolean recovery;
  Message pendingReq;

  /**
   * Constructs a TPCLog to log Messages from the TPCNode.
   *
   * @param logPath path to location of log file for this server
   * @param node The TPCNode the log based on
   */
  public TPCLog(String logPath, TPCNode node)  {
    this.logPath = logPath;
    this.node = node;
    this.entries = new ArrayList<Message>();
    //rebuildServer();
    File logFile = new File(logPath);
    // log found, recover from log
    if(logFile.exists() && !logFile.isDirectory()) {
      recovery = true;
      rebuildServer();
    } else {
      recovery = false;
    }
  }

  /**
   * Add an entry to the log and flush the entire log to disk.
   */
  public void appendAndFlush(Message entry) {
    entries.add(entry);
    flushToDisk();
  }

  /**
   * Get last entry in the log.
   * @return last entry put into the log
   */
  public Message getLastEntry() {
    if (entries.size() > 0) {
      return entries.get(entries.size() - 1);
    }
    return null;
  }

  /**
   * Get last vote request which has no preceding 
   * Abort or Commit
   * @return last entry put into the log
   */
  public Message getLastVoteReq() {
    int i = entries.size() - 1;
    while (i >= 0) {
      if (entries.get(i).isVoteReq()) {
        return entries.get(i);
      } else if (entries.get(i).isAbort() || entries.get(i).isCommit()) {
        return null;
      }
      i--;
    }
    return null;
  }
  
  
  /**
   * Load log from persistent storage at logPath.
   */
  @SuppressWarnings("unchecked")
  public void loadFromDisk() {
    ObjectInputStream inputStream = null;

    try {
      inputStream = new ObjectInputStream(new FileInputStream(logPath));
      entries = (ArrayList<Message>) inputStream.readObject();
    } catch (Exception e) {
    } finally {
      // if log did not exist, creating empty entries list
      if (entries == null) {
        entries = new ArrayList<Message>();
      }

      try {
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Writes the log to persistent storage at logPath.
   */
  public void flushToDisk() {
    ObjectOutputStream outputStream = null;

    try {
      outputStream = new ObjectOutputStream(new FileOutputStream(logPath));
      outputStream.writeObject(entries);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (outputStream != null) {
          outputStream.flush();
          outputStream.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Load log and rebuild TPCNode state and playList by iterating over log entries. 
   *
   */
  public void rebuildServer(){
    loadFromDisk();
    System.out.println("Rebuilding server ...");
    System.out.println("Initial state: " + node.state.name());
    for (Message m : entries) {
      m.print();
      if (m.isVoteReq()) {
        pendingReq = m;
        node.state = TPCNode.SlaveState.ABORTED;
      } else if (m.isAbort()) {
        pendingReq = null;
        node.state = TPCNode.SlaveState.ABORTED;
      } else if (m.isCommit()) {
        node.execute(pendingReq);
        node.state = TPCNode.SlaveState.COMMITTED;
      } else if (m.isPreCommit()) {
        assert node.state == TPCNode.SlaveState.UNCERTAIN;
        node.state = TPCNode.SlaveState.COMMITTABLE;
      } else if (m.isResponse()) {
        if (m.votedYes()) {
          node.state = TPCNode.SlaveState.UNCERTAIN;
        } else if (m.votedNo()) {   // voted no, just abort
          pendingReq = null;
          node.state = TPCNode.SlaveState.ABORTED;
        }
      }
      System.out.println("Current state: " + node.state.name());
    }
    System.out.println("Rebuild server finished!!");
    lastStep();
  }
  
  public void lastStep() {
    switch (node.state) {
    case UNCERTAIN:
    case COMMITTABLE: 
      System.out.println("Unable to recover by itself ...");
      System.out.println("Let's ask for help!");
      node.broadcast(new Message(Constants.STATE_QUERY));
      try {
        Thread.sleep(node.getSleepTime());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      lastStep();  // keep trying ...
      break;
    case ABORTED:
      System.out.println("I am back aborted!! LOL...");
      if (pendingReq != null) {
        appendAndFlush(new Message(Constants.ABORT));
        pendingReq = null;
      }
      node.printPlayList();
      break;
    case COMMITTED:
      System.out.println("I am back commited!! LOL...");
      if (pendingReq != null) {
        node.execute(pendingReq);
        appendAndFlush(new Message(Constants.COMMIT));
      }
      node.printPlayList();
    }
    node.broadcast(new Message(Constants.JOIN_REQ));
  }
}
