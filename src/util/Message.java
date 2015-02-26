package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

public class Message {
  private String msgType;
  private String src;
  private String dst;
  private String song;
  private String url;
  private String message;
  
  
  
  public String marshal() {
    StringBuilder sb = new StringBuilder();
    sb.append("msgType = " + msgType + '\n');
    sb.append("src = " + src + '\n');
    sb.append("dst = " + dst + '\n');
    sb.append("song = " + song + '\n');
    sb.append("url = " + url + '\n');
    sb.append("message = " + message + '\n');
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
