package explorer.scheduler;

import com.google.gson.annotations.Expose;
import explorer.ExplorerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtils;

import javax.xml.soap.Node;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NodeFailureSettings extends SchedulerSettings {
  private static final Logger log = LoggerFactory.getLogger(NodeFailureSettings.class);
  ExplorerConf conf = ExplorerConf.getInstance();

  // derived from the parameters
  public final int NUM_MAJORITY= (conf.NUM_PROCESSES / 2) + 1;

  private Random random;
  public final int depth = conf.bugDepth;

  @Expose
  public int seed = conf.randomSeed;
  @Expose
  private List<NodeFailureSettings.NodeFailure> failures = new ArrayList<>();
  @Expose
  public int mutationNo = 0; // the mutations of the same seed are enumerated for logging

  private int numMutators = 0;

  // does not introduce any failures prior to execution, failures introduced during runtime
  public static NodeFailureSettings ONLINE_CONTROLLED = new NodeFailureSettings(new ArrayList<>());

  // to be used for deserialization (failures will be set)
  public NodeFailureSettings() {
    this(ExplorerConf.getInstance().randomSeed);
  }

  // to be used for creation
  public NodeFailureSettings(int seed) {
    this.seed = seed;
    random = new Random(seed);
    // TODO:
//    failures = getRandomFailures();
    // failures = getTestFailures();
    failures = getFailuresToReproduceBug();
    String failuresAsStr = toString();
    log.info("Failure Injecting Settings: \n" + failuresAsStr);
  }

  public List<NodeFailure> getTestFailures() {
    List<NodeFailureSettings.NodeFailure> f  = new ArrayList<>();
    f.add(new NodeFailureSettings.NodeFailure(0, 4, 0));
//    f.add(new NodeFailureSettings.NodeFailure(0, 5, 1));
    f.add(new NodeFailureSettings.NodeFailure(0, 4, 2));
//    f.add(new NodeFailureSettings.NodeFailure(0, 5, 2));
//    f.add(new NodeFailureSettings.NodeFailure(0, 2, 2));
//    f.add(new NodeFailureSettings.NodeFailure(0, 3, 1));
//    f.add(new NodeFailureSettings.NodeFailure(0, 4, 2));
//    f.add(new NodeFailureSettings.NodeFailure(0, 5, 1));
//    f.add(new NodeFailureSettings.NodeFailure(1, 4, 0));
//    f.add(new NodeFailureSettings.NodeFailure(2, 0, 0));
//    f.add(new NodeFailureSettings.NodeFailure(2, 0, 1));
//    f.add(new NodeFailureSettings.NodeFailure(3, 0, 0));
    return f;
  }

  public NodeFailureSettings(int seed, int failPhase, int failRound, String failNodeId){
    this.seed = seed;
    String failNodeArr[] = failNodeId.split(",");
    failures = getCertainFailures(failPhase, failRound, failNodeArr);
    String failuresAsStr = toString();
    log.info("Failure Injecting Settings: \n" + failuresAsStr);
  }


  public NodeFailureSettings(List<NodeFailure> failures) {
    this.failures = failures;
  }

  // constructor used when constructed from a mutation - written as json for next executions
  private NodeFailureSettings(int seed, List<NodeFailure> failures, int mutationNo) {
    random = new Random(seed);
    this.mutationNo = mutationNo;
    this.failures = failures;
  }


  /**
   * Not used in the current version of the algorithm/tester
   * @return mutated failure settings for another test
   */
  @Override
  public SchedulerSettings mutate() {
    int failureToRemove = random.nextInt(failures.size());

    List<NodeFailure> mutation = new ArrayList<>(failures);
    mutation.remove(failureToRemove);

    // add existing:
    int[] failurePerPhase = new int[conf.NUM_PHASES];
    List<Integer> phases = new ArrayList<>();

    for(int i = 0; i < conf.NUM_PHASES; i++) {
      phases.add(i);
    }

    for (NodeFailure nodeFailure : mutation) {
      int phaseToFailAt = nodeFailure.k;
      failurePerPhase[phaseToFailAt]++;
      if (failurePerPhase[phaseToFailAt] == conf.NUM_PROCESSES)
        phases.remove(phaseToFailAt);
    }

    int phaseToFailAt = random.nextInt(phases.size());
    int roundToFailAt = random.nextInt(conf.NUM_ROUNDS_IN_PROTOCOL);
    int processToFail = random.nextInt(conf.NUM_PROCESSES);

    mutation.add(new NodeFailure(phaseToFailAt, roundToFailAt, processToFail));

    return new NodeFailureSettings(conf.randomSeed, mutation, ++numMutators);
  }

  private List<NodeFailureSettings.NodeFailure> getRandomFailures() {
    if(depth > 0 )
      return getBoundedRandomFailures(depth);
    else
      return getUnboundedRandomFailures();
  }

  private List<NodeFailureSettings.NodeFailure> getBoundedRandomFailures(int d) {
    List<NodeFailureSettings.NodeFailure> f  = new ArrayList<>();

    int[] failurePerPhase = new int[conf.NUM_PHASES];
    List<Integer> phases = new ArrayList<>();

    for(int i = 0; i < conf.NUM_PHASES; i++) {
      phases.add(i);
    }

    for(int i = 0; i < d; i++) {
      int phaseToFailAt = random.nextInt(phases.size());
      failurePerPhase[phaseToFailAt] ++;
      if(failurePerPhase[phaseToFailAt] == conf.NUM_PROCESSES)
        phases.remove(phaseToFailAt);

      int roundToFailAt = random.nextInt(conf.NUM_ROUNDS_IN_PROTOCOL);
      int processToFail = random.nextInt(conf.NUM_PROCESSES);

      f.add(new NodeFailure(phaseToFailAt, roundToFailAt, processToFail));
    }

    return f;
  }

  // TODO:
  public List<NodeFailureSettings.NodeFailure> getCertainFailures(int phaseToFailAt, int roundToFailAt, String[] failNodeArr){
    List<NodeFailureSettings.NodeFailure> f  = new ArrayList<>();
    for(int i = 0; i < failNodeArr.length; i++){
      String failNodePerRound[] = failNodeArr[i].split("-");
      for(int j = 0; j < failNodePerRound.length; j++){
        if(!failNodePerRound[j].equals("3"))
//          System.out.println(i/6 + " " + i%6 + " " + failNodePerRound[j]);
          f.add(new NodeFailure(i / 6, i % 6, Integer.parseInt(failNodePerRound[j])));
      }
    }
    return f;
  }

  private List<NodeFailureSettings.NodeFailure> getUnboundedRandomFailures() {
    List<NodeFailureSettings.NodeFailure> f  = new ArrayList<>();

    // for each phase, select a set of processes to fail, at a selected round
    for(int i = 0; i < conf.NUM_PHASES; i++) {
      for(int j = 0; j < conf.NUM_PROCESSES; j++) {
        if(random.nextDouble() > 0) {
          int roundNum = random.nextInt(6);
          f.add(new NodeFailure(i, roundNum, j));
        }
      }
    }
    return f;
  }

  private List<NodeFailureSettings.NodeFailure> getMutationFailures() {
    // read mutations file
    List<String> mutationStrs = FileUtils.readLinesFromFile("mutations");
    List<NodeFailureSettings.NodeFailure> failuresToExecute = new ArrayList<>();

    if(mutationStrs.size() > 0) {
      // take the first mutation to execute
      failuresToExecute = ((NodeFailureSettings)toObject(mutationStrs.get(0))).getFailures();

      // write back the rest
      //todo revise with a more efficient way
      StringBuilder sb = new StringBuilder();
      for(int i = 1; i < mutationStrs.size(); i++) {
        sb.append(mutationStrs.get(i)).append("\n");
      }
      FileUtils.writeToFile("mutations", sb.toString(), false);
      FileUtils.writeToFile("result.txt", "\nRunning mutation: " + mutationStrs.get(0), true);
    }

    return failuresToExecute;
  }

  public List<NodeFailureSettings.NodeFailure> getFailures() {
    return failures;
  }

  public String toString() {
    if(this.equals(NodeFailureSettings.ONLINE_CONTROLLED))
      return "Online Controlled Failure Settings";

    StringBuffer sb = new StringBuffer();
    sb.append("Num processes: ").append(conf.NUM_PROCESSES).append("\n");
    sb.append("Num rounds in the protocol: ").append(conf.NUM_ROUNDS_IN_PROTOCOL).append("\n");
    sb.append("Num requests/phases: ").append(conf.NUM_PHASES).append("\n");
    sb.append("Link establishment period: ").append(conf.linkEstablishmentPeriod).append("\n");
    sb.append("Random seed: ").append(conf.randomSeed).append("\n");
    sb.append("Bug depth: ").append(conf.bugDepth).append("\n");
    return sb.toString();
  }

  private List<NodeFailureSettings.NodeFailure> getFailuresToReproduceBug() {
    // depth is 6
    List<NodeFailureSettings.NodeFailure> f  = new ArrayList<>();
    f.add(new NodeFailure(0, 4, 2));
    f.add(new NodeFailure(1, 2, 2));
    f.add(new NodeFailure(1, 4, 0));
    f.add(new NodeFailure(2, 0, 0));
    f.add(new NodeFailure(2, 0, 1));
//    f.add(new NodeFailure(3, 0, 0));
    return f;
  }

  public static class NodeFailure {
    @Expose
    int k; // in which request does it happen?
    @Expose
    int r; // at which round does it happen?
    @Expose
    int process; // which process fails?

    public NodeFailure(int k, int r, int p) {
      this.k = k;
      this.r = r;
      this.process = p;
    }

    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof NodeFailure)) return false;

      return k == ((NodeFailure)obj).k
              && r == ((NodeFailure)obj).r
              && process == ((NodeFailure)obj).process;
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + k;
      result = 31 * result + r;
      result = 31 * result + process;
      return result;
    }

    public String toString() {
      return "k:" + k + " r:" + r + " proc:" + process;
    }
  };




}