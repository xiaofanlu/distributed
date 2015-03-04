package tpc;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import util.Message;

public class TPCLog {

  private String logPath;
  private TPCNode server;
  private ArrayList<Message> entries;

  /**
   * Constructs a TPCLog to log Messages from the TPCNode.
   *
   * @param logPath path to location of log file for this server
   * @param ?? TODO
   */
  public TPCLog(String logPath, TPCNode server)  {
    this.logPath = logPath;
    this.server = server;
    this.entries = new ArrayList<Message>();
    rebuildServer();
  }

  /**
   * Add an entry to the log and flush the entire log to disk.
   * You do not have to efficiently append entries onto the log stored on disk.
   *
   * @param entry KVMessage to write to the log
   */
  public void appendAndFlush(Message entry) {
    entries.add(entry);
    flushToDisk();
  }

  /**
   * Get last entry in the log.
   *
   * @return last entry put into the log
   */
  public Message getLastEntry() {
    if (entries.size() > 0) {
      return entries.get(entries.size() - 1);
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
   * Load log and rebuild KVServer by iterating over log entries. You do not
   * need to restore the previous cache state (i.e. ignore GETS).
   *
   * @throws KVException if an error occurs in KVServer (though we expect none)
   */
  public void rebuildServer(){
    loadFromDisk();
    System.out.println("To do: rebuild server ...");
  }

}
