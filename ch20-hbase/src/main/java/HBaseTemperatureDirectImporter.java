import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Używa klasy {@link HTable} do bezpośredniego wczytywania danych o temperaturze do tabeli w bazie HBase.
 * Mniej wydajne od klasy {@link HBaseTemperatureImporter} (ponieważ włączony jest automatyczny zapis do pliku), a 
 * zwłaszcza od klasy {@link HBaseTemperatureBulkImporter}.
 */
public class HBaseTemperatureDirectImporter extends Configured implements Tool {
  
  static class HBaseTemperatureMapper<K, V> extends Mapper<LongWritable, Text, K, V> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private HTable table;

    @Override
    protected void setup(Context context) throws IOException {
      // Tworzenie obiektu typu HTable raz na początku i utrzymywanie go, zamiast
      // generowania go przy każdym mapowaniu.
      this.table = new HTable(HBaseConfiguration.create(context.getConfiguration()),
          "observations");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
      parser.parse(value.toString());
      if (parser.isValidTemperature()) {
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
            parser.getObservationDate().getTime());
        Put p = new Put(rowKey);
        p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
            HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
            Bytes.toBytes(parser.getAirTemperature()));
        table.put(p);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException {
      table.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Użytkowanie: HBaseTemperatureDirectImporter <dane_wejściowe>");
      return -1;
    }
    Job job = new Job(getConf(), getClass().getSimpleName());
    job.setJarByClass(getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setMapperClass(HBaseTemperatureMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(HBaseConfiguration.create(),
        new HBaseTemperatureDirectImporter(), args);
    System.exit(exitCode);
  }
}