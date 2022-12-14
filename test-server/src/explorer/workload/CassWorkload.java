package explorer.workload;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import explorer.ExplorerConf;
import explorer.verifier.CassVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

class CassWorkload {

  private static Logger log = LoggerFactory.getLogger(CassWorkload.class);

  public static final int clusterPort = ExplorerConf.getInstance().clusterPort;
  public static final int poolTimeoutMillis = ExplorerConf.getInstance().poolTimeoutMillis;
  public static final int readTimeoutMillis = ExplorerConf.getInstance().clusterPort;
  public static final int timeBetweenQueriesMillis = ExplorerConf.getInstance().timeBetweenQueriesMillis;

  static void execute6023() {
    try {
      executeCql(0, "test", "UPDATE tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1'");
      Thread.sleep(timeBetweenQueriesMillis);
      new CassVerifier().writeLog();

      executeCql(1, "test", "UPDATE tests SET value_1 = 'B', value_2 = 'B' WHERE name = 'testing' IF  value_1 = 'A'");
      Thread.sleep(timeBetweenQueriesMillis);
      new CassVerifier().writeLog();

      executeCql(2, "test", "UPDATE tests SET value_3 = 'C' WHERE name = 'testing' IF owner = 'user_1'");
      Thread.sleep(timeBetweenQueriesMillis);
      new CassVerifier().writeLog();

    } catch (InterruptedException e) {
      log.error("Interrupted while sleeping", e);
    }
//    executeCql(0, "test", "UPDATE tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1'");
//    new CassVerifier().writeLog();
//
//    executeCql(1, "test", "UPDATE tests SET value_1 = 'B', value_2 = 'B' WHERE name = 'testing' IF  value_1 = 'A'");
//    new CassVerifier().writeLog();
//
//    executeCql(2, "test", "UPDATE tests SET value_3 = 'C' WHERE name = 'testing' IF owner = 'user_1'");
//    new CassVerifier().writeLog();
  }

  static void reset6023() {
      executeCql(1, "test", "UPDATE tests SET value_1 = 'value_1', value_2 = 'value_2' WHERE name = 'testing'");
  }

  static void submitQuery(int nodeId, String query) {
    executeCql(nodeId, "test", query);
    try {
      Thread.sleep(timeBetweenQueriesMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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
  static void submitQueries(List<Integer> nodeIds, List<String> queries) {
    if(nodeIds.size() != queries.size()) {
      log.error("The number of nodes to submit is not equal to the number of the queries.");
      System.exit(-1);
    }

    for(int i = 0; i < nodeIds.size(); i++) {
      try {
        executeCql(nodeIds.get(i), "test", queries.get(i));
        //executeCql(0, "test", "UPDATE tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1';");//.get();
        Thread.sleep(timeBetweenQueriesMillis);
      } catch (InterruptedException e) {
        log.error("Interrupted while sleeping", e);
      }
    }

  }

  private static boolean executeCql(int nodeId, String keyspace, String cql) {
    try (Cluster cluster = getCluster(nodeId).init(); Session session = cluster.connect(keyspace).init()) {
      log.info("Executing query for cluster {}: {}", nodeId, cql);
      ResultSet resultSet = session.execute(cql);
//      WriteTo("RESULT"+resultSet.wasApplied());
      return resultSet.wasApplied();
    } catch (Exception ex) {
      log.warn("=== Error with communication to node {}", nodeId, ex);
    }
    return false;
  }

  private static Cluster getCluster(int nodeId) {
    String nodeIp = CassNodeConfig.address(nodeId);
    Cluster cluster = Cluster.builder()
        .addContactPoint(nodeIp)
        .withProtocolVersion(ProtocolVersion.V2)
        .withPort(clusterPort)
        .withRetryPolicy(new CustomRetryPolicy(1, 1, 1)) // should retry same host
        .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), Collections.singleton(new InetSocketAddress(nodeIp, clusterPort))))
        .build();
    cluster.getConfiguration().getPoolingOptions().setPoolTimeoutMillis(poolTimeoutMillis);
    cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(readTimeoutMillis);
    return cluster;
  }

  static class CustomRetryPolicy implements ExtendedRetryPolicy {

    private final int readAttempts;
    private final int writeAttempts;
    private final int unavailableAttempts;

    private final int errorAttempts = 1;

    public CustomRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
      this.readAttempts = readAttempts;
      this.writeAttempts = writeAttempts;
      this.unavailableAttempts = unavailableAttempts;
    }

    @Override
    public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataReceived, int rTime) {
      if (dataReceived) {
        return RetryDecision.ignore();
      } else if (rTime < readAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      } else {
        return RetryDecision.rethrow();
      }
    }

    @Override
    public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
      if (wTime < writeAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      }
      return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
      if (uTime < unavailableAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      }
      return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry) {
      log.info("Timeout - Request Error");
      if (nbRetry < errorAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      }
      return RetryDecision.ignore();
    }
  }
}
