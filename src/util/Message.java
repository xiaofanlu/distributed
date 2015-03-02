package util;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Properties;

import tpc.TPCNode;

public class Message implements Serializable {
  private String msgType;
  private String src = "-1";
  private String dst = "-1";
  private String song;
  private String url;
  private String message;
  
  public static final long serialVersionUID = 6473128480951955693L;

  
  public Message() {
    this("");
  }
  
  public Message (String type) {
    this(type,  "");
  }
  
  public Message (String type, String m) {
    this(type, "",  "", m);
  }
  
  public Message (String type, String key, String val) {
    this(type, key,  val, "");
  }
  
  public Message (String type,  String key, String val, String m) {
    msgType = type;
    song = key;
    url = val;
    message = m;
  }
  
  public boolean isVoteReq() {
    return msgType.equals(Constants.VOTE_REQ);
  }
  
  public boolean isStateReq() {
    return msgType.equals(Constants.STATE_REQ);
  }
  
  public boolean isAck() {
    return msgType.equals(Constants.ACK);
  }
  public boolean isResponse() {
    return msgType.equals(Constants.RESP);
  }
  
  public boolean votedNo() {
    return msgType.equals(Constants.RESP) && message.equals(Constants.NO);
  }
  
  public boolean votedYes() {
    System.out.println(">>" + message + "<<");
    return msgType.equals(Constants.RESP) && message.equals(Constants.YES);
  }
  
  public boolean isFeedback() {
    return msgType.equals(Constants.COMMIT) ||
        msgType.equals(Constants.PRECOMMIT) ||
      msgType.equals(Constants.ABORT);
  }
  
  public TPCNode.SlaveState getState() {
    if (!msgType.equals(Constants.STATE_REP)) {
      return TPCNode.SlaveState.READY;  // irrelevent state
    }
    return TPCNode.SlaveState.valueOf(message);
  }
  
  
  public void setSrc(int s) {
    src = "" + s;
  }

  public void setDst(int d) {
    dst = "" + d;
  }
  
  public String getType() {
    return msgType;
  }
  
  public int getSrc() {
    try {
      return Integer.parseInt(src);
    } catch (NumberFormatException e) {
      return -1;
    }
  }
  
  public int getDst() {
    try {
      return Integer.parseInt(dst);
    } catch (NumberFormatException e) {
      return -1;
    }
  }
  
  public String getSong() {
    return song;
  }
  
  public String getUrl() {
    return url;
  }
  
  public String getMessage() {
    return message;
  }
  
  public String marshal() {
    StringBuilder sb = new StringBuilder();
    sb.append("msgType = " + msgType + '\n');
    sb.append("src = " + src + '\n');
    sb.append("dst = " + dst + '\n');
    sb.append("song = " + song + '\n');
    sb.append("url = " + url + '\n');
    sb.append("message = " + message);
    return sb.toString();
  }
  
  public void unmarshal (String str) {
    Properties prop = new Properties();
    try {
      prop.load(new StringReader(str));
      msgType = prop.getProperty("msgType");
      src = prop.getProperty("src");
      dst = prop.getProperty("dst");
      song = prop.getProperty("song");
      url = prop.getProperty("url");
      message = prop.getProperty("message");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
}
