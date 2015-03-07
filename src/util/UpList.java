package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeSet;

import tpc.TPCNode;

public class UpList {
  public TreeSet<Integer> upList; // current view of alive nodes
  public String path;
  private TPCNode node;

  public HashMap<Integer, TreeSet<Integer>> uLog;
  public TreeSet<Integer> intersection;
  public TreeSet<Integer> myLog;  // logged uplist, not current view
  public TreeSet<Integer> recoverGroup;  // node that has statequery

  public UpList(TPCNode node) {
    this.node = node;
    path = "TPCUpList" + node.config.procNum + ".txt";
    uLog = new HashMap<Integer, TreeSet<Integer>> ();
    //intersection = new TreeSet<Integer> ();

    
    File f = new File(path);
    // log found, recover from log, otherwise, start new
    if(f.exists() && !f.isDirectory()) {
      Scanner sc;
      try {
        sc = new Scanner(f);
        if (sc.hasNextLine()) {
          myLog = parseString(sc.nextLine());
          intersection = new TreeSet<Integer> (myLog);
          recoverGroup = new TreeSet<Integer> ();
          recoverGroup.add(node.getProcNum());
        }
        sc.close();
      } catch (FileNotFoundException e) {
        buildNewList(node.config.numProcesses);
      }
      upList = new TreeSet<Integer> ();
      upList.add(node.getProcNum());
    } else {
      buildNewList(node.config.numProcesses);
      logToFile();
    }
  }

  public void buildNewList(int numProcesses) {
    upList = new TreeSet<Integer> ();
    for (int i = 0; i < numProcesses; i++) {
      upList.add(i);
    }
  }

  public TreeSet<Integer> parseString(String list) {
    TreeSet<Integer> rst = new TreeSet<Integer> ();
    String[] items = list.split("\\$");
    for (String item : items) {
      rst.add(Integer.parseInt(item));
    }
    return rst;
  }


  public void updateFromString(String list) {
    upList = parseString(list);
  }

  public void logToFile() {
    try {
      PrintWriter writer = new PrintWriter(path, "UTF-8");
      writer.println(marshal());
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public void add(int i) {
    upList.add(i);
    logToFile();
  }

  public void remove(int i) {
    upList.remove(i);
    logToFile();
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
    logToFile();
  }

  public TreeSet<Integer> getBroadcastList() {
    TreeSet<Integer> rst = new TreeSet<Integer> (upList);
    rst.remove(node.getProcNum());
    return rst;
  }


  public int getMaster () {
    int master = Integer.MAX_VALUE;
    for (int i : upList) {
      master = Math.min(master, i);
    }
    return master;
  }

  public String serialize (TreeSet<Integer> set) {
    StringBuilder sb = new StringBuilder();
    for (int i : set) {
      sb.append(i + "$");
    }
    if (set.size() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public String marshal () {
    return serialize(upList);
  }
  
  public String getMyLogUpList() {
    return serialize(myLog);
  }

  public void print() {
    System.out.println();
    System.out.println("++++++++++++++UpList++++++++++++");
    for (int i : upList) {
      if (i == node.getMaster()) {
        System.out.print(i + "(M) ");
      } else {
        System.out.print(i + " ");
      }
    }
    System.out.println();
    System.out.println("+++++++++++++++ End  +++++++++++++");
    System.out.println();
  }
}
