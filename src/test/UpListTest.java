package test;

import static org.junit.Assert.*;

import org.junit.Test;

import util.UpList;

public class UpListTest {
  @Test
  public void test1 () {
    UpList ul = new UpList();
    ul.add(1);
    ul.add(2);
    ul.add(0);
    assertEquals(ul.getMaster(), 0);
    assertEquals(ul.marshal(), "0$1$2");
  }
  
 
}
