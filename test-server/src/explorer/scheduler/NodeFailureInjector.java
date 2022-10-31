package explorer.scheduler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import explorer.ExplorerConf;
import explorer.PaxosEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * The NodeFailureInjector injects failures into the synchronous non-faulty executions of a system
 * (The current version introduces only node failures where a failing node cannot receive/send any messages)
 */
public class NodeFailureInjector extends Scheduler {
  private static final Logger log = LoggerFactory.getLogger(NodeFailureInjector.class);
  ExplorerConf conf = ExplorerConf.getInstance();

  // variables maintaining the current state of the protocol execution
  private int currentRound;     // between 0 to (NUM_LIVENESS_ROUNDS-1)
  private int currentPhase;    // between 0 to (NUM_PHASES - 1)
  private int toExecuteInCurRound;
  private int executedInCurRound;
  private int droppedFromNextRound; // incremented for the response events in case request events are dropped

  private final int period;  // period of clearing failed processes (set to NUM_LIVENESS_ROUNDS)

  private List<NodeFailureSettings.NodeFailure> failures;
  private Set<Integer> failedProcesses;

  private List<PaxosEvent.ProtocolRound> rounds;
  // not needed by the standard algorithm, used for optimization (eliminating timeouts for collecting the messages in a round) for Cassandra
  //private List<String> ballots = Arrays.asList("33d9f0f0-08c5-11e7-845e-", "33da1800-08c5-11e7-845e-", "33da3f10-08c5-11e7-845e-", "33da6620-08c5-11e7-845e-", "33da8d30-08c5-11e7-845e-");
  private List<String> ballots;

  List<String> mess;

  // for stats:
  int numSuccessfulRounds = 0, numSuccessfulPhases = 0;

  public NodeFailureInjector(NodeFailureSettings settings) {
    this.settings = settings;
    failures = new ArrayList<>(settings.getFailures());

    failedProcesses = new HashSet<>();
    ballots = new ArrayList<>();
    rounds = new ArrayList<>();
    rounds.add(PaxosEvent.ProtocolRound.PAXOS_PREPARE);
    currentRound = 0;
    currentPhase = 0;
    toExecuteInCurRound = conf.NUM_PROCESSES;
    executedInCurRound = 0;
    droppedFromNextRound = 0;

    period = conf.linkEstablishmentPeriod;
    numTotalRounds = 0;

    //TODO:
    mess = new ArrayList<>();

    if(settings.equals(NodeFailureSettings.ONLINE_CONTROLLED)) {
      log.debug("Using online control of the failing nodes.");
      if(ExplorerConf.getInstance().logResult)
        FileUtils.writeToFile(ExplorerConf.getInstance().resultFile, "Using online control of the failing nodes.", true);
    } else {
      log.debug("Using failures: " + settings.getFailures() + " seed: " + settings.seed);
      if(ExplorerConf.getInstance().logResult)
        FileUtils.writeToFile(ExplorerConf.getInstance().resultFile, "failNodeId:" + conf.failNodeId +  "  Seed for failures: " + settings.seed, true);
    }
  }

  @Override
  synchronized public void addNewEvent(int connectionId, PaxosEvent message){
    super.addNewEvent(connectionId, message);

    if(message.getVerb().equals("PAXOS_PREPARE") && ballots.size() == currentPhase) {
      ballots.add(message.getBallot());
      //log.debug("Added:  " + message.getBallot());
    }

    checkForSchedule();
  }

  @Override
  synchronized protected void checkForSchedule(){
    Set<PaxosEvent> keys = events.keySet();
    for(PaxosEvent m: keys) {
//      System.out.print("m:");
//      System.out.println(m);

      if(isOfCurrentRound(m)) {
        if(isToDrop(m)) {
          log.info("Dropped message: " + m.toString() + " " + m.getPayload());
          events.remove(m);
          toExecuteInCurRound --;
          if(m.isRequest()) droppedFromNextRound ++;
        } else {
          log.debug("=== Scheduling:  " + m);
          if(ballots.get(currentPhase) == null)
            ballots.add(currentPhase, m.getBallot());
          schedule(m);
          executedInCurRound ++;
        }
        try{
          checkUpdateRound();
        }catch (IOException e){
          e.printStackTrace();
        }
        checkForSchedule();
        return;
      }
    }
  }

  // The current phase is determined by the ballot numbers and round of the protocol step
  // After the PREPARE of the first request,
  //   all the messages regarding that request are executed before SCHEDULING the PREPARE of another one
  // has side effect!
  synchronized private boolean isToDrop(PaxosEvent message) {
    assert(isOfCurrentRound(message));
    int processOfMessage = (int)(message.isRequest() ? message.getRecv() : message.getSender());
    if(failedProcesses.contains(processOfMessage)) return true;

    NodeFailureSettings.NodeFailure match = null;
    for(NodeFailureSettings.NodeFailure nf: failures) {
      if(nf.k == currentPhase && rounds.get(currentRound).ordinal() == nf.r && nf.process == processOfMessage) {
        match = nf;
        break;
      }
    }

    if(match != null) {
      failures.remove(match);
      failedProcesses.add(match.process);
      return true;
    }
    mess.add(message.toString());
    return false;
  }

  // the standard algorithm increases the rounds by just collecting messages sent in the round - this is an optimization
  synchronized private void checkUpdateRound() throws IOException {
    if((toExecuteInCurRound - executedInCurRound) == 0) { // move to next round
      currentRound++;
      numTotalRounds++;
      if (executedInCurRound >= ((NodeFailureSettings) settings).NUM_MAJORITY) numSuccessfulRounds++;
//      System.out.println(currentRound);
      // inform coverage strategy
      //coverageStrategy.onRoundComplete(rounds.get(currentRound-1).toString(), failedProcesses);
      // TODO: it's the time to enter the next round:
//       if(conf.failPhase != -1 && !conf.isFinish && currentRound == (conf.failPhase * 6 + conf.failRound + 1)){
//         String state = "" + Integer.toString(currentPhase) + " " + Integer.toString(currentRound) + " " + Integer.toString(toExecuteInCurRound) + " " + Integer.toString(executedInCurRound) + " " + droppedFromNextRound;
//         try{
//           String fileName = "failStatePhase" + Integer.toString(conf.failPhase) + "Round" + Integer.toString(conf.failRound);
//           File file = new File("/home/xie/explorer-server/test-server/" + fileName);
//           if(!file.exists()){
//             file.createNewFile();
//           }
//           if(!conf.failStateMap.containsValue(state)){
//             conf.failStateMap.put(conf.failNodeId, state);
//             System.out.println(conf.failNodeId);
//           }
//           FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
//           BufferedWriter bw = new BufferedWriter(fw);
//           for(Map.Entry<String, String> entry: conf.failStateMap.entrySet()){
//             bw.write(entry.getKey() + "|" + entry.getValue()+"\n");
//           }
//           bw.close();
//           fw.close();
//
//           conf.isFinish = true;
//
//         }catch (IOException e){
//           e.printStackTrace();
//         }
//      }


      // update the next round - state machine
      switch(rounds.get(currentRound-1)) {
        case PAXOS_PREPARE:
          if(toExecuteInCurRound < ((NodeFailureSettings)settings).NUM_MAJORITY) {
            coverageStrategy.onRequestPhaseComplete(rounds.get(currentRound-1).toString(), failedProcesses);
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PREPARE_RESPONSE);
            toExecuteInCurRound = conf.NUM_PROCESSES - droppedFromNextRound;
          } else {
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PREPARE_RESPONSE);
            toExecuteInCurRound = conf.NUM_PROCESSES - droppedFromNextRound;
          }
          break;
        case PAXOS_PREPARE_RESPONSE:
          //TODO:
//          Cluster cluster = getCluster(nodeId).init()
//          Cluster cluster = getCluster(0).

          if(toExecuteInCurRound < ((NodeFailureSettings)settings).NUM_MAJORITY) {
            coverageStrategy.onRequestPhaseComplete(rounds.get(currentRound-1).toString(), failedProcesses);
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PREPARE);
            numTotalRounds += 4;
            currentPhase ++;
            toExecuteInCurRound = conf.NUM_PROCESSES;
          } else {
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PROPOSE);
            toExecuteInCurRound = conf.NUM_PROCESSES;
          }
          break;
        case PAXOS_PROPOSE: //fixed:
          if(toExecuteInCurRound < ((NodeFailureSettings)settings).NUM_MAJORITY) {
            coverageStrategy.onRequestPhaseComplete(rounds.get(currentRound-1).toString(), failedProcesses);
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PROPOSE_RESPONSE);
            toExecuteInCurRound = conf.NUM_PROCESSES - droppedFromNextRound;
          } else {
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PROPOSE_RESPONSE);
            toExecuteInCurRound = conf.NUM_PROCESSES - droppedFromNextRound;
          }
          break;
        case PAXOS_PROPOSE_RESPONSE: //todo make it more accurate with replies! (even with majority of replies, we can turn back to PREPARE)
          if(toExecuteInCurRound < ((NodeFailureSettings)settings).NUM_MAJORITY) {
            coverageStrategy.onRequestPhaseComplete(rounds.get(currentRound-1).toString(), failedProcesses);
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_PREPARE);
            numTotalRounds += 2;
            currentPhase ++;
          }
          else rounds.add(PaxosEvent.ProtocolRound.PAXOS_COMMIT);
          toExecuteInCurRound = conf.NUM_PROCESSES;
          break;
        case PAXOS_COMMIT:
          if(toExecuteInCurRound < ((NodeFailureSettings)settings).NUM_MAJORITY) {
            coverageStrategy.onRequestPhaseComplete(rounds.get(currentRound-1).toString(), failedProcesses);
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_COMMIT_RESPONSE);
            toExecuteInCurRound = conf.NUM_PROCESSES - droppedFromNextRound;
          } else {
            rounds.add(PaxosEvent.ProtocolRound.PAXOS_COMMIT_RESPONSE);
            toExecuteInCurRound = conf.NUM_PROCESSES - droppedFromNextRound;
            numSuccessfulPhases ++;
          }
          break;
        case PAXOS_COMMIT_RESPONSE:
          coverageStrategy.onRequestPhaseComplete(rounds.get(currentRound-1).toString(), failedProcesses);
          rounds.add(PaxosEvent.ProtocolRound.PAXOS_PREPARE);
          toExecuteInCurRound = conf.NUM_PROCESSES;
          currentPhase ++;
          break;
        default:
          log.error("Invalid protocol state");
      }

//      System.out.println("OUT:"+currentRound+" "+currentPhase+" "+toExecuteInCurRound+" "+ executedInCurRound + " ");
//      System.out.println();
//      System.out.println(rounds.get(currentRound-1));


      // update values related to failures for the current round:
      executedInCurRound = 0;
      droppedFromNextRound = 0;

      // reset the quorum after each period number of rounds
      if(numTotalRounds % period == 0)
        failedProcesses.clear();

    }
  }

  // Customized for Cassandra example - the default way of detecting the messages in a round is to collect messages for some timeout
  synchronized private boolean isOfCurrentPhase(PaxosEvent m) {
    boolean isPrepareOfCurrentPhase = (m.getVerb().equals(PaxosEvent.ProtocolRound.PAXOS_PREPARE.toString()) && m.getBallot().equals(ballots.get(currentPhase)));
    boolean isNonPrepare = !m.getVerb().equals(PaxosEvent.ProtocolRound.PAXOS_PREPARE.toString()); // if the preceding messages are scheduled, it is of current phase
    return isPrepareOfCurrentPhase || isNonPrepare;
  }

  // Customized for Cassandra example - the default way of detecting the messages in a round is to collect messages for some timeout
  synchronized private boolean isOfCurrentRound(PaxosEvent m) {
    return isOfCurrentPhase(m) && m.getVerb().equals(rounds.get(currentRound).toString());
  }

  @Override
  public boolean isScheduleCompleted() {
    // executions that completed max number of rounds are completed
    if(numTotalRounds >= conf.NUM_MAX_ROUNDS) {
      log.info("Hit the max number of rounds, returning.");
      return true;
    }

    if(numSuccessfulRounds == conf.NUM_REQUESTS) {
      log.info("Hit the max number of rounds, returning.");
      return true;
    }

    return false;
  }

  Object o = new Object(); // used for notification of the client

  public String getFailuresAsStr() {
    return ((NodeFailureSettings)settings).getFailures().toString();
  }

  @Override
  public String getStats() {
    StringBuffer sb = new StringBuffer();
    sb.append("Num successful rounds: ").append(numSuccessfulRounds).append("\n");
    sb.append("Num rounds: ").append(numTotalRounds).append("\n");
    sb.append("Num successful phases: ").append(numSuccessfulPhases).append("\n");
    sb.append("Num phases: ").append(currentPhase).append("\n");
    sb.append("Num messages: ").append(scheduled.size()).append("\n");
//    sb.append("FailNodeId: ").append(conf.failNodeId).append("\n");

    if(conf.logSchedule) sb.append(getScheduleAsStr());
    return sb.toString();
  }

}
