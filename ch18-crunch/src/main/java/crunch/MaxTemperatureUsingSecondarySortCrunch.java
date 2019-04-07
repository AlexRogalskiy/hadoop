package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;

import static org.apache.crunch.lib.Sort.Order.ASCENDING;
import static org.apache.crunch.lib.Sort.Order.DESCENDING;
import static org.apache.crunch.lib.Sort.ColumnOrder.by;
import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.pairs;

// Przeznaczona dla Cruncha wersja programu MaxTemperatureUsingSecondarySort z rozdziału 9.
public class MaxTemperatureUsingSecondarySortCrunch {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Użytkowanie: MaxTemperatureUsingSecondarySortCrunch <ścieżka wejściowa> <ścieżka wyjściowa>");
      System.exit(-1);
    }
    Pipeline pipeline = new MRPipeline(MaxTemperatureUsingSecondarySortCrunch.class);
    PCollection<String> records = pipeline.readTextFile(args[0]);
    PCollection<Pair<Integer, Integer>> pairs = records
        .parallelDo(toYearTempValueFn(), pairs(ints(), ints()));

    PCollection<Pair<Integer, Integer>> sorted =
        Sort.sortPairs(pairs, by(1, ASCENDING), by(2, DESCENDING));
    pipeline.writeTextFile(sorted, args[1]);
    pipeline.run();
  }

  private static DoFn<String, Pair<Integer, Integer>> toYearTempValueFn() {
    return new DoFn<String, Pair<Integer, Integer>>() {
      NcdcRecordParser parser = new NcdcRecordParser();
      @Override
      public void process(String input, Emitter<Pair<Integer, Integer>> emitter) {
        parser.parse(input);
        if (parser.isValidTemperature()) {
          emitter.emit(Pair.of(parser.getYearInt(), parser.getAirTemperature()));
        }
      }
    };
  }
}
