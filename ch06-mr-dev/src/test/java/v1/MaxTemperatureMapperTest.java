package v1;
// cc MaxTemperatureMapperTestV1 Test jednostkowy klasy MaxTemperatureMapper
// == MaxTemperatureMapperTestV1Missing
// vv MaxTemperatureMapperTestV1
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class MaxTemperatureMapperTest {

  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Rok  ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperatura ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
// ^^ MaxTemperatureMapperTestV1
  @Ignore // Ponieważ w książce prezentowany jest test kończący się niepowodzeniem
// vv MaxTemperatureMapperTestV1Missing
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Rok  ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperatura ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .runTest();
  }
// ^^ MaxTemperatureMapperTestV1Missing
  @Test
  public void processesMalformedTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Rok  ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperatura ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInput(new LongWritable(0), value)
      .withOutput(new Text("1957"), new IntWritable(1957))
      .runTest();
  }
// vv MaxTemperatureMapperTestV1
}
// ^^ MaxTemperatureMapperTestV1
