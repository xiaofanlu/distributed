package util;

import java.util.Arrays;
import java.util.TreeSet;

public class UpList {
  public TreeSet<Integer> upList;
  
  public UpList() {
    upList = new TreeSet<Integer> ();
  }
  
  public UpList(String list) {
    this();
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
