package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MissingTemperatureFields extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      JobBuilder.printUsage(this, "<identyfikator zadania>");
      return -1;
    }
    String jobID = args[0];
    JobClient jobClient = new JobClient(new JobConf(getConf()));
    RunningJob job = jobClient.getJob(JobID.forName(jobID));
    if (job == null) {
      System.err.printf("Brak zadania o podanym identyfikatorze %s.\n", jobID);
      return -1;
    }
    if (!job.isComplete()) {
      System.err.printf("Zadanie %s nie ukończyło pracy.\n", jobID);
      return -1;
    }

    Counters counters = job.getCounters();
    long missing = counters.getCounter(
        MaxTemperatureWithCounters.Temperature.MISSING);

    long total = counters.getCounter(Task.Counter.MAP_INPUT_RECORDS);

    System.out.printf("Rekordy bez pola z temperaturą: %.2f%%\n",
        100.0 * missing / total);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
    System.exit(exitCode);
  }
}