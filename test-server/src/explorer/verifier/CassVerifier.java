package explorer.verifier;

import java.util.Map;
import java.util.HashMap;
import com.datastax.driver.core.*;
import explorer.ExplorerConf;
import utils.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class CassVerifier  {
  private Map<String, String> map = new HashMap<String, String>();

  private String value_1;
  private String value_2;
  private String value_3;

  public boolean verify() {
    return checkDataConsistency();
  }

  public void writeLog(){
      getValues();
      value_1 = map.get("value_1");
      value_2 = map.get("value_2");
      value_3 = map.get("value_3");

       WriteTo("log+5+" + "null+"+ value_1 + value_2 + value_3+"+null");
  }
    public static synchronized void WriteTo(String mes){
        String filePath = "/home/xie/explorer-server/test/test1.txt";
        FileWriter fw = null;
        try
        {
            File file = new File(filePath);
            if (!file.exists())
            {
                file.createNewFile();
            }
            fw = new FileWriter(filePath, true);
            BufferedWriter bw=new BufferedWriter(fw);
            bw.write(mes + "\n");
            bw.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
  private boolean checkDataConsistency() {
	  getValues();

    try {
      value_1 = map.get("value_1");
      value_2 = map.get("value_2");

      System.out.println("Value1: " + value_1 + "    Value2: " + value_2);
      if (value_1.equals("A") && (value_2.equals("B"))) {
        System.out.println("Reproduced the bug.");

        if(ExplorerConf.getInstance().logResult) {
            FileUtils.writeToFile(ExplorerConf.getInstance().resultFile, "Reproduced the bug.", true);
        }

        return false;
      }
    } catch (Exception e) {
      System.out.println("Failed to check data consistency.");
      System.out.println(e.getMessage());
      return true;
    }
    return true;
  }

  private void getValues(){
		Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build();
		Session session = cluster.connect("test");
		try {
			//System.out.println("Querying row from table");
			ResultSet rs = session.execute("SELECT * FROM tests");
			//System.out.println("Row acquired");
			Row row = rs.one();
            map.put("owner", row.getString("owner"));
            map.put("value_1", row.getString("value_1"));
            map.put("value_2", row.getString("value_2"));
            map.put("value_3", row.getString("value_3"));
		} catch (Exception e) {
			System.out.println("ERROR in reading row.");
			System.out.println(e.getMessage());
		} finally {
		  session.close();
		  cluster.close();
    }
  }

}
