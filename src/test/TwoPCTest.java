package test;

import static org.junit.Assert.*;

import org.junit.Test;

import tpc.TPCNode;

public class TwoPCTest {
  /*
  @Test
  public void MasterTest1 () {
    TPCMaster ms = new TPCMaster("config0.txt");
    assertEquals(ms.getProcNum(), 0);
    assertEquals(ms.broadcastList.size(), 2);
  }
  */
  
  
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Usage: TwoPCTest $i");
      System.exit(0);
    }
    
    String fileName = "config" + args[0] + ".txt";
    TPCNode ms = new TPCNode(fileName);
  }
}
