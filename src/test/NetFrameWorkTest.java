package test;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import framework.Config;
import framework.NetController;

public class NetFrameWorkTest {
  @Test
  public void test1() throws FileNotFoundException, IOException {
    Config c0 = new Config("config0.txt");
    NetController nc0 = new NetController(c0);
    Config c1 = new Config("config1.txt");
    NetController nc1 = new NetController(c1);
    
    nc0.sendMsg(1, "from 0 to 1, how are u?");
    nc0.sendMsg(1, "from 0 to 1, how are u?");


    
    for (String str : nc0.getReceivedMsgs()) {
      System.out.println(str);
    }
    //nc0.shutdown(); 
    nc1.sendMsg(0, "from 1 to 0, how are u?");
    nc1.sendMsg(0, "from 1 to 0, how are u?");
    for (String str : nc1.getReceivedMsgs()) {
      System.out.println(str);
    }
  }
}
