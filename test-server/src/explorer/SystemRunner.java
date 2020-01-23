package explorer;

import explorer.net.Handler;
import explorer.net.TestingServer;
import explorer.net.socket.SocketServer;
import explorer.scheduler.*;
import explorer.verifier.CassVerifier;
import explorer.workload.CassWorkloadDriver;
import explorer.workload.WorkloadDriver;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import utils.FileUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

/**
 * Runs Cassandra with the specifies scheduler and parameters
 * (Does not use TestDriver API which provides convenient methods for injecting faults via FailureInjectingScheduler)
 *
 * The arguments in the ExplorerConf can be overwritten by providing arguments:
 * e.g. radnomSeed=12347265 linkEstablishmentPeriod = 6
 */
public class SystemRunner {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);

        ExplorerConf conf = ExplorerConf.initialize("explorer.conf", args);

        String failureSettingsJsonStr = "";
        List<String> options = Arrays.asList(args);
        if(options.contains("failures")) {
          try {
              failureSettingsJsonStr = failureSettingsJsonStr =options.get(options.indexOf("failures") + 1);

          } catch (Exception e) {
            throw new RuntimeException("Invalid command line arguments.\n" + e.getMessage());
          }
        }

        runAll(conf, failureSettingsJsonStr);
    }

    public static void runAll(ExplorerConf conf, String failureSettingsJsonStr) throws Exception {
        try {
            Scheduler scheduler = null;
            SchedulerSettings settings = null;
            Class<? extends Scheduler> schedulerClass = (Class<? extends Scheduler>) Class.forName(conf.schedulerClass);

            switch(conf.schedulerClass) {
                case "explorer.scheduler.NodeFailureInjector":
                    if(failureSettingsJsonStr != null && !failureSettingsJsonStr.isEmpty())
                        settings = NodeFailureSettings.toObject(failureSettingsJsonStr);
                    else
                        settings = new NodeFailureSettings(conf.randomSeed);
                    //System.out.println(settings.toJsonStr());
                    scheduler = schedulerClass.getConstructor(NodeFailureSettings.class).newInstance(settings);
                    break;
                case "explorer.scheduler.LinkFailureInjector":
                    if(failureSettingsJsonStr != null && !failureSettingsJsonStr.isEmpty())
                        settings = LinkFailureSettings.toObject(failureSettingsJsonStr);
                    else
                        settings = new LinkFailureSettings(conf.randomSeed);
                    //System.out.println(settings.toJsonStr());
                    scheduler = schedulerClass.getConstructor(LinkFailureSettings.class).newInstance(settings);
                    break;
            }

            runAll(conf, scheduler);

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
            | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void runAll(ExplorerConf conf, Scheduler scheduler) throws Exception {
        //CoverageStrategy coverageStrategy = new LastCliquesStrategy();
        //scheduler.setCoverageStrategy(coverageStrategy);

        Handler handler = new ConnectionHandler(scheduler);

        // start server which enforces a schedule over distributed system nodes
        TestingServer testingServer = new SocketServer(conf.portNumber, conf.numberOfClients, handler);

        Thread serverThread = new Thread(testingServer, "testing-server");
        serverThread.start();

        // start distributed system nodes and the workload
        WorkloadDriver workloadDriver = new CassWorkloadDriver(conf.getWorkloadDirs(), conf.numberOfClients, conf.javaPath);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            workloadDriver.stopEnsemble();
            testingServer.stop();
        }));

        // send workload
        workloadDriver.cleanup();
        workloadDriver.prepare(1);
        workloadDriver.startEnsemble();
        Thread.sleep(4000);

        // send workload
        workloadDriver.sendWorkload();


        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(ExplorerConf.getInstance().maxExecutionDuration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Shutting down");
                FileUtils.writeToFile(ExplorerConf.getInstance().resultFile, "Timed out - shutting down", true);
                System.exit(-1);
            }
        });
        t.start();

        while(!scheduler.isExecutionCompleted())
        {
            Thread.sleep(250);
        }
        FileUtils.writeToFile(conf.resultFile, scheduler.getStats(), true);
        if(!new CassVerifier().verify()) {
            if(scheduler instanceof NodeFailureInjector)
                FileUtils.writeToFile(ExplorerConf.getInstance().resultFile, "Failures: " + ((NodeFailureInjector)scheduler).getFailuresAsStr(), true);
        }
        workloadDriver.stopEnsemble();
        Thread.sleep(1000);
        t.stop();
        testingServer.stop();
        serverThread.join();
    }

}