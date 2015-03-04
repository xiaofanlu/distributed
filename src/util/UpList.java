package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.TreeSet;

import framework.Config;

public class UpList {
  public TreeSet<Integer> upList;
  public String path;
  private FileWriter fileWriter;
  private BufferedWriter outStream;
  
  public UpList () {
    upList = new TreeSet<Integer> ();
  }
  
  public UpList(Config config) {
    //path = "TPCUpList" + config.getProcNum() + ".txt";

  }
  
  public UpList(int numProcesses) {
    upList = new TreeSet<Integer> ();
    for (int i = 0; i < numProcesses; i++) {
      upList.add(i);
    }
  }
  
  public UpList(String list) {
    upList = new TreeSet<Integer> ();
    String[] items = list.split("\\$");
    //System.out.println(Arrays.toString(items));
    for (String item : items) {
      upList.add(Integer.parseInt(item));
    }
  }
  
  public void add(int i) {
    upList.add(i);
  }
  
  public void remove(int i) {
    upList.remove(i);
  }
  
  public int size() {
    return upList.size() - 1;
  }
  
  public void setMaster (int id) {
    TreeSet<Integer> list = new TreeSet<Integer> ();
    for (int i : upList) {
      if (i >= id) {
        list.add(i);
      }
    }
    upList = list;
  }
  
  
  public int getMaster () {
    int master = Integer.MAX_VALUE;
    for (int i : upList) {
      master = Math.min(master, i);
    }
    return master;
  }
  
  public String marshal () {
    StringBuilder sb = new StringBuilder();
    for (int i : upList) {
      sb.append(i + "$");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }
}
