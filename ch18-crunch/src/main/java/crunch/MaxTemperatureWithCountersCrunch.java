package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

// Przeznaczona dla Cruncha wersja programów MaxTemperatureWithCountersCrunch i  MissingTemperatureFields z rozdziału 9.
// Warto zauważyć, że w naturalny sposób łączą się one w jeden program
public class MaxTemperatureWithCountersCrunch {
  
  enum Temperature {
    MISSING,
    MALFORMED
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Użytkowanie: MaxTemperatureWithCountersCrunch <ścieżka wejściowa> <ścieżka wyjściowa>");
      System.exit(-1);
    }

    Pipeline pipeline = new MRPipeline(MaxTemperatureWithCountersCrunch.class);
    PCollection<String> records = pipeline.readTextFile(args[0]);
    long total = records.getSize();
    
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(Aggregators.MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, args[1]);
    PipelineResult result = pipeline.run();
    if (result.succeeded()) {
      for (StageResult stageResult : result.getStageResults()) {
        System.out.println(stageResult.getStageName());
        long missing = stageResult.getCounterValue(Temperature.MISSING);
        long malformed = stageResult.getCounterValue(Temperature.MALFORMED);
        System.out.println("Brakujące: " + missing);
        System.out.println("Błędne: " + malformed);
        System.out.println("Razem: " + total);
        System.out.printf("Procent rekordów bez pola z temperaturą: %.2f%%\n",
            100.0 * missing / total);
      }
    }
  }

  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getYear(), parser.getAirTemperature()));
        } else if (parser.isMalformedTemperature()) {
          setStatus("Ignorowanie potencjalnie błędnych danych wejściowych: " + input);
          increment(Temperature.MALFORMED);
        } else if (parser.isMissingTemperature()) {
          increment(Temperature.MISSING);
        }

        // Dynamiczny licznik
        increment("TemperatureQuality", parser.getQuality(), 1);
      }
    };
  }

}
