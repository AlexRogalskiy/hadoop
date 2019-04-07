package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Przeznaczona dla Cruncha wersja programu MaxTemperature z rozdziału 2.
public class MaxTemperatureCrunchWithShutdownHook {
  
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Użytkowanie: MaxTemperatureCrunchWithShutdownHook <ścieżka wejściowa> <ścieżka wyjściowa>");
      System.exit(-1);
    }

    Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchWithShutdownHook.class);
    PCollection<String> records = pipeline.readTextFile(args[0]);

    PTable<String, Integer> yearTemperatures = records
        .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()));
    PTable<String, Integer> maxTemps = yearTemperatures
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, args[1]);
    final PipelineExecution execution = pipeline.runAsync();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Zatrzymywanie potoku...");
          execution.kill();
          execution.waitUntilDone();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }));
    execution.waitUntilDone();
    System.exit(execution.getResult().succeeded() ? 0 : 1);
  }

  static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        }
      }
    };
  }

}
