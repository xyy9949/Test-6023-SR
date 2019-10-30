package explorer.workload;

import java.io.IOException;

public interface WorkloadDriver {
  void prepare(int testId) throws Exception;
  void startEnsemble();
  void sendWorkload();
  void sendResetWorkload();
  void prepareNextTest();
  void stopEnsemble();
  void cleanup() throws IOException;
}
