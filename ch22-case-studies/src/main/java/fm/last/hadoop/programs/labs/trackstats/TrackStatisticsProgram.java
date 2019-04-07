/*
 * Copyright 2008 Last.fm.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.hadoop.programs.labs.trackstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import fm.last.hadoop.io.records.TrackStats;

/**
 * Program obliczający różne statystyki związane z utworami na podstawie surowych danych o odtwarzaniu.
 */
public class TrackStatisticsProgram {

  public static final Log log = LogFactory.getLog(TrackStatisticsProgram.class);

  // Poniższe wartości wyznaczają pozycje elementów w surowych danych

  private static final int COL_USERID = 0;
  private static final int COL_TRACKID = 1;
  private static final int COL_SCROBBLES = 2;
  private static final int COL_RADIO = 3;
  private static final int COL_SKIP = 4;

  private Configuration conf;

  /**
   * Tworzy nowy obiekt TrackStatisticsProgram z wykorzystaniem domyślnego obiektu Configuration.
   */
  public TrackStatisticsProgram() {
    this.conf = new Configuration();
  }

  /**
   * Wyliczenie z licznikami błędów Hadoopa.
   */
  private enum COUNTER_KEYS {
    INVALID_LINES, NOT_LISTEN
  };

  /**
   * Mapper przyjmujący surowe dane o odtwarzaniu i wyświetlający liczbę unikatowych słuchaczy utworów.
   */
  public static class UniqueListenersMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, IntWritable> output,
        Reporter reporter) throws IOException {

      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // Jeśli wiersz jest pusty, należy zgłosić błąd i zignorować dane
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" "); // Ogranicznikiem w surowych danych są spacje
      try {
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radioListens = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        if (scrobbles <= 0 && radioListens <= 0) {
          // Jeśli utwór był odtwarzany zero razy, należy zgłosić błąd i zignorować dane
          reporter.incrCounter(COUNTER_KEYS.NOT_LISTEN, 1);
          return;
        }
		// Jeśli kod dotarł do tego miejsca, użytkownik odsłuchał utworu, dlatego należy dodać
		// identyfikator użytkownika do identyfikatora utworu
        IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]));
        IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_USERID]));
        output.collect(trackId, userId);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        reporter.setStatus("Błędny wiersz w danych o odtwarzaniu: " + rawLine);
        return;
      }
    }
  }

  /**
   * Funkcja łącząca zwiększająca wydajność dzięki usuwanie powtarzających się identyfikatorów
   * użytkownika z danych wyjściowych mapper.
   */
  public static class UniqueListenersCombiner extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      Set<IntWritable> userIds = new HashSet<IntWritable>();
      while (values.hasNext()) {
        IntWritable userId = values.next();
        if (!userIds.contains(userId)) {
		  // Jeśli użytkownik nie został oznaczony jako słuchacz utworzy, należy dodać go do zbioru i zwrócić
          userIds.add(new IntWritable(userId.get()));
          output.collect(trackId, userId);
        }
      }
    }
  }

  /**
   * Reduktor zwracający tylko unikatowe identyfikatory słuchaczy dla utworów (usuwa powtórzenia). Ostateczne dane
   * wyjściowe to liczba unikatowych słuchaczy poszczególnych utworów.
   */
  public static class UniqueListenersReducer extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      Set<Integer> userIds = new HashSet<Integer>();
	  // Dodawanie wszystkich identyfikatorów użytkowników do zbioru. Powtórzenia są
	  // automatycznie usuwane (zobacz kontrakt)
      while (values.hasNext()) {
        IntWritable userId = values.next();
        userIds.add(Integer.valueOf(userId.get()));
      }
      // Zwraca odwzorowania trackId -> liczba unikatowych słuchaczy każdego utworu
      output.collect(trackId, new IntWritable(userIds.size()));
    }

  }

  /**
   * Mapper podsumowujący różne statystyki dla utworów. Dane wejściowe to surowe dane o odsłuchach. Dane wyjściowe są
   * częściowo zapełniane w obiekcie TrackStatistics powiązanym z identyfikatorem utworu
   */
  public static class SumMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TrackStats> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, TrackStats> output,
        Reporter reporter) throws IOException {

      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // Ignorowanie pustych wierszy
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" ");
      try {
        int trackId = Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]);
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radio = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        int skip = Integer.parseInt(parts[TrackStatisticsProgram.COL_SKIP]);
		// Ustawianie liczby słuchaczy na 0 (jest ona obliczana później) i innych wartości 
		// na podstawie pliku tekstowego
        TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
        output.collect(new IntWritable(trackId), trackstat);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        log.warn("Nieprawidłowy wiersz w danych o odsłuchach: " + rawLine);
      }
    }
  }

  /**
   * Sumuje statystyki dla utworów. Dane wyjściowe to obiekt TrackStatistics dla identyfikatora utworu.
   */
  public static class SumReducer extends MapReduceBase implements
      Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

    @Override
    public void reduce(IntWritable trackId, Iterator<TrackStats> values,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats sum = new TrackStats(); // Przechowuje sumaryczne wartości dla danego utworu
      while (values.hasNext()) {
        TrackStats trackStats = (TrackStats) values.next();
        sum.setListeners(sum.getListeners() + trackStats.getListeners());
        sum.setPlays(sum.getPlays() + trackStats.getPlays());
        sum.setSkips(sum.getSkips() + trackStats.getSkips());
        sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
        sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
      }
      output.collect(trackId, sum);
    }
  }

  /**
   * Mapper przyjmujący liczbę słuchaczy utworu i przekształcający ją na obiekt TrackStats zwracany
   * dla każdego identyfikatora utworu.
   */
  public static class MergeListenersMapper extends MapReduceBase implements
      Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {

    public void map(IntWritable trackId, IntWritable uniqueListenerCount,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats trackStats = new TrackStats();
      trackStats.setListeners(uniqueListenerCount.get());
      output.collect(trackId, trackStats);
    }
  }

  /**
   * Tworzenie obiektu JobConf dla zadania, które ustala liczbę unikatowych słuchaczy dla utworów.
   * 
   * @param inputDir Ścieżka do katalogu z plikami z surowymi danymi o odsłuchach.
   * @return Konfigurację zadania do zliczania unikatowych słuchaczy.
   */
  private JobConf getUniqueListenersJobConf(Path inputDir) {
    log.info("Tworzenie konfiguracji dla zadania zwracającego liczbę unikatowych słuchaczy");
	// Zwracanie wyników do tymczasowego katalogu pośredniego usuwanego przez metodę start()
    Path uniqueListenersOutput = new Path("uniqueListeners");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // Id utworu
    conf.setOutputValueClass(IntWritable.class); // Liczba unikatowych słuchaczy
    conf.setInputFormat(TextInputFormat.class); // Surowe dane o odsłuchach
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(UniqueListenersMapper.class);
    conf.setCombinerClass(UniqueListenersCombiner.class);
    conf.setReducerClass(UniqueListenersReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, uniqueListenersOutput);
    conf.setJobName("uniqueListeners");
    return conf;
  }

  /**
   * Tworzy konfigurację zadania, które sumuje statystyki dla utworów.
   * 
   * @param inputDir Ścieżka do katalogu z plikami z surowymi danymi wejściowymi.
   * @return Konfiguracja dla zadania sumującego statystyki.
   */
  private JobConf getSumJobConf(Path inputDir) {
    log.info("Tworzenie konfiguracji dla zadania sumującego statystyki");
    // Zwracanie wyników do tymczasowego katalogu pośredniego usuwanego przez metodę start()
	
    Path playsOutput = new Path("sum");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // Id utworu
    conf.setOutputValueClass(TrackStats.class); // Statystyki utworu
    conf.setInputFormat(TextInputFormat.class); // Surowe dane o odsłuchach
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(SumMapper.class);
    conf.setCombinerClass(SumReducer.class);
    conf.setReducerClass(SumReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, playsOutput);
    conf.setJobName("sum");
    return conf;
  }

  /**
   * Tworzy konfigurację zadania, które scala unikatowych słuchaczy i statystyki utworów.
   * 
   * @param outputPath Ścieżka, w której zapisywane są wyniki.
   * @param sumInputDir Ścieżka z danymi z zadania sumującego statystyki.
   * @param listenersInputDir Ścieżka z danymi z zadania zliczającego unikatowych słuchaczy.
   * @return Konfiguracja zadania scalającego dane.
   */
  private JobConf getMergeConf(Path outputPath, Path sumInputDir, Path listenersInputDir) {
    log.info("Tworzenie konfiguracji dla zadania scalającego dane");
    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // Id utworu
    conf.setOutputValueClass(TrackStats.class); // Ogólne statystyki dla utworu
    conf.setCombinerClass(SumReducer.class); // Można bezpiecznie wykorzystać reduktor jako klasę złączającą
    conf.setReducerClass(SumReducer.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileOutputFormat.setOutputPath(conf, outputPath);

    MultipleInputs.addInputPath(conf, sumInputDir, SequenceFileInputFormat.class, IdentityMapper.class);
    MultipleInputs.addInputPath(conf, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);
    conf.setJobName("merge");
    return conf;
  }

  /**
   * Uruchamianie programu.
   * 
   * @param inputDir Ścieżka do katalogu z plikami z surowymi danymi o odsłuchach.
   * @param outputPath Ścieżka, w której zapisane zostaną wyniki.
   * @throws IOException Zgłaszany, gdy wystąpi błąd przy pobieraniu danych z systemu plików lub w trakcie pracy zadania.
   */
  public void start(Path inputDir, Path outputDir) throws IOException {
    FileSystem fs = FileSystem.get(this.conf);

    JobConf uniqueListenersConf = getUniqueListenersJobConf(inputDir);
    Path listenersOutputDir = FileOutputFormat.getOutputPath(uniqueListenersConf);
    Job listenersJob = new Job(uniqueListenersConf);
	// Usuwanie danych wyjściowych z poprzednich uruchomień zadania
    if (fs.exists(FileOutputFormat.getOutputPath(uniqueListenersConf))) {
      fs.delete(FileOutputFormat.getOutputPath(uniqueListenersConf), true);
    }

    JobConf sumConf = getSumJobConf(inputDir);
    Path sumOutputDir = FileOutputFormat.getOutputPath(sumConf);
    Job sumJob = new Job(sumConf);
	// Usuwanie danych wyjściowych z poprzednich uruchomień zadania
    if (fs.exists(FileOutputFormat.getOutputPath(sumConf))) {
      fs.delete(FileOutputFormat.getOutputPath(sumConf), true);
    }

	// Zadanie scalające dane jest zależne od dwóch pozostałych zadań
    ArrayList<Job> mergeDependencies = new ArrayList<Job>();
    mergeDependencies.add(listenersJob);
    mergeDependencies.add(sumJob);
    JobConf mergeConf = getMergeConf(outputDir, sumOutputDir, listenersOutputDir);
    Job mergeJob = new Job(mergeConf, mergeDependencies);
	// Usuwanie danych wyjściowych z poprzednich uruchomień zadania
    if (fs.exists(FileOutputFormat.getOutputPath(mergeConf))) {
      fs.delete(FileOutputFormat.getOutputPath(mergeConf), true);
    }

	// Zapisywanie ścieżek wyjściowych zadań pośrednich, co pozwala usunąć dane pośrednie po udanym zakończeniu pracy programu
    List<Path> deletePaths = new ArrayList<Path>();
    deletePaths.add(FileOutputFormat.getOutputPath(uniqueListenersConf));
    deletePaths.add(FileOutputFormat.getOutputPath(sumConf));

    JobControl control = new JobControl("TrackStatisticsProgram");
    control.addJob(listenersJob);
    control.addJob(sumJob);
    control.addJob(mergeJob);

    // Wykonywanie zadań
    try {
      Thread jobControlThread = new Thread(control, "jobcontrol");
      jobControlThread.start();
      while (!control.allFinished()) {
        Thread.sleep(1000);
      }
      if (control.getFailedJobs().size() > 0) {
        throw new IOException("Przynajmniej jedno zadanie zakończone niepowodzeniem");
      }
    } catch (InterruptedException e) {
      throw new IOException("Nastąpiło przerwanie w trakcie oczekiwania na zakończenie wątku jobcontrol", e);
    }

	// Usuwanie pośrednich ścieżek wyjściowych
    for (Path deletePath : deletePaths) {
      fs.delete(deletePath, true);
    }
  }

  /**
   * Ustawianie konfiguracji dla programu.
   * 
   * @param conf Nowa konfiguracja dla programu.
   */
  public void setConf(Configuration conf) {
    this.conf = conf; // Ustawiane zwykle tylko w testach jednostkowych.
  }

  /**
   * Pobiera konfigurację programu.
   * 
   * @return Konfiguracja programu.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Metoda main używana do uruchamiania programu TrackStatisticsProgram z poziomu wiersza poleceń. Przyjmuje dwa
   * parametry - ścieżkę do katalogu z surowymi danymi wejściowymi i ścieżkę na dane wyjściowe.
   * 
   * @param args Argumenty z wiersza poleceń.
   * @throws IOException Zgłaszany, jeśli w trakcie wykonywania programu wystąpił błąd.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      log.info("Argumenty: <katalog z danymi wejściowymi> <katalog na dane wyjściowe>");
      return;
    }

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    log.info("Uruchamianie dla katalogów wejściowych: " + inputPath);
    TrackStatisticsProgram listeners = new TrackStatisticsProgram();
    listeners.start(inputPath, outputDir);
  }

}
