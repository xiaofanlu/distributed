package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;
import java.util.TreeSet;

import tpc.TPCNode;

public class UpList {
  public TreeSet<Integer> upList;
  public String path;
  private TPCNode node;


  public UpList(TPCNode node) {
    this.node = node;
    path = "TPCUpList" + node.config.procNum + ".txt";
    File f = new File(path);
    // log found, recover from log, otherwise, start new
    if(f.exists() && !f.isDirectory()) {
      Scanner sc;
      try {
        sc = new Scanner(f);
        if (sc.hasNextLine()) {
          updateFromString(sc.nextLine());
        }
        sc.close();
      } catch (FileNotFoundException e) {
        buildNewList(node.config.numProcesses);
      }
    } else {
      buildNewList(node.config.numProcesses);
    }
  }

  public void buildNewList(int numProcesses) {
    upList = new TreeSet<Integer> ();
    for (int i = 0; i < numProcesses; i++) {
      upList.add(i);
    }
  }

  public void updateFromString(String list) {
    upList = new TreeSet<Integer> ();
    String[] items = list.split("\\$");
    //System.out.println(Arrays.toString(items));
    for (String item : items) {
      upList.add(Integer.parseInt(item));
    }
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
    if (upList.size() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
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
