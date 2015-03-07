/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package framework;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;

public class Config {

  /**
   * Loads config from a file.  Optionally puts in 'procNum' if in file.
   * See sample file for syntax
   * @param filename
   * @throws FileNotFoundException
   * @throws IOException
   */
  public Config(String filename) throws FileNotFoundException, IOException {
    logger = Logger.getLogger("NetFramework");

    Properties prop = new Properties();
    prop.load(new FileInputStream(filename));
    numProcesses = loadInt(prop,"NumProcesses");
    addresses = new InetAddress[numProcesses];
    ports = new int[numProcesses];
    for (int i = 0; i < numProcesses; i++) {
      ports[i] = loadInt(prop, "port" + i);
      addresses[i] = InetAddress.getByName(prop.getProperty("host" + i).trim());
      //System.out.printf("%d: %d @ %s\n", i, ports[i], addresses[i]);
    }

    if (prop.getProperty("ProcNum") != null) {
      procNum = loadInt(prop,"ProcNum");
    } else {
      logger.info("procNum not loaded from file");
    }

    map = new HashMap<String, Integer> ();
    // default value 
    map.put("partialCommit", -1);
    map.put("partialPreCommit", -1);
    map.put("deathAfterProcess", -1);
    map.put("deathAfterCount", Integer.MAX_VALUE);
    map.put("delay", 1);

    System.out.println();
    System.out.println(" >>>>>>>>>>>>>>> TEST SETTING <<<<<<<<<<<<<<<< ");
    for (String pName : map.keySet()) {
      if (prop.getProperty(pName) != null) {
        map.put(pName, loadInt(prop, pName));
        if (map.get(pName) != -1) {
          System.out.println("\t" + pName.toUpperCase() + "\t: " 
              + map.get(pName));
        }
      }
    }
    System.out.println(" >>>>>>>>>>>>>>>>    END     <<<<<<<<<<<<<<<<< ");
    System.out.println("\n");
  }

  private int loadInt(Properties prop, String s) {
    return Integer.parseInt(prop.getProperty(s.trim()));
  }

  public int get(String pName) {
    if (map.containsKey(pName)) {
      return map.get(pName);
    }
    return -1;
  }


  /**
   * Default constructor for those who want to populate config file manually
   */
  public Config() {
  }

  /**
   * Array of addresses of other hosts.  All hosts should have identical info here.
   */
  public InetAddress[] addresses;


  /**
   * Array of listening port of other hosts.  All hosts should have identical info here.
   */
  public int[] ports;

  /**
   * Total number of hosts
   */
  public int numProcesses;

  /**
   * This hosts number (should correspond to array above).  Each host should have a different number.
   */
  public int procNum;

  /**
   * Logger.  Mainly used for console printing, though be diverted to a file.
   * Verbosity can be restricted by raising level to WARN
   */
  public Logger logger;


  /**
   * - partialCommit <n>
   * When the process becomes coordinator, she will only send the commit broadcast to process
   * n and then crash...
   * 
   */

  /**
   * - partialPreCommit <n>
   * When the process becomes coordinator, she will only send the precommit broadcast to process
   * n and then crash...
   * 
   */

  /**
   * - deathAfter <n> <p>
   * Process kills itself after received n messages from process p
   */


  /*
   * -delay <n> 
   * Slow down the protocol to allow for human observation and intervention.
   */
  public HashMap<String, Integer> map;

}
