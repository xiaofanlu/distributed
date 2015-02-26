package test;

import static org.junit.Assert.*;

import org.junit.Test;

import util.Message;
import util.Constants;


public class MessageTest {

  @Test
  public void testMarshal1 () {
    Message m = new Message (Constants.ABORT);
    assertEquals(m.marshal(), "msgType = abort\nsrc = -1\ndst = -1\nsong = \nurl = \nmessage = ");
  }
  
  @Test
  public void testMarshal2 () {
    Message m = new Message (Constants.COMMIT, "haha");
    assertEquals(m.marshal(), "msgType = commit\nsrc = -1\ndst = -1\nsong = \nurl = \nmessage = haha");
  }
  
  @Test
  public void testUnMarshal1 () {
    String in = "msgType = abort\nsrc = 1\ndst = 9\nsong = google\nurl = google.com\nmessage = ";
    Message m = new Message ();
    m.unmarshal(in);
    assertEquals(m.getType(), Constants.ABORT);
    assertEquals(m.getSrc(), 1);
    assertEquals(m.getDst(), 9);
    assertEquals(m.getSong(), "google");
    assertEquals(m.getUrl(), "google.com");
    assertEquals(m.getMessage(), ""); 
  }
  
}
