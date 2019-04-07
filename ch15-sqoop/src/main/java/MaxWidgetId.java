
import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class MaxWidgetId extends Configured implements Tool {

  public static class MaxWidgetMapper
      extends Mapper<LongWritable, Text, LongWritable, Widget> {

    private Widget maxWidget = null;

    public void map(LongWritable k, Text v, Context context) {
      Widget widget = new Widget();
      try {
        widget.parse(v); // Automatycznie wygenerowane. Parsowanie wszystkich pól z tekstu.
      } catch (ParseError pe) {
        // Otrzymano błędnie uformowany rekord. Ignorowanie go.
        return;
      }

      Integer id = widget.get_id();
      if (null == id) {
        return;
      } else {
        if (maxWidget == null
            || id.intValue() > maxWidget.get_id().intValue()) {
          maxWidget = widget;
        }
      }
    }

    public void cleanup(Context context)
        throws IOException, InterruptedException {
      if (null != maxWidget) {
        context.write(new LongWritable(0), maxWidget);
      }
    }
  }

  public static class MaxWidgetReducer
      extends Reducer<LongWritable, Widget, Widget, NullWritable> {

	// Używane jest jedno wywołanie reduce z kluczem '0', co powoduje pobranie
	// maksymalnego obiektu Widget z każdej operacji mapowania. Pobieranie 
	// maksymalnego obiektu z tej listy.
    public void reduce(LongWritable k, Iterable<Widget> vals, Context context)
        throws IOException, InterruptedException {
      Widget maxWidget = null;

      for (Widget w : vals) {
        if (maxWidget == null
            || w.get_id().intValue() > maxWidget.get_id().intValue()) {
          try {
            maxWidget = (Widget) w.clone();
          } catch (CloneNotSupportedException cnse) {
            // Nie powinien wystąpić; klasy wygenerowane przez Sqoopa obsługują metodę clone().
            throw new IOException(cnse);
          }
        }
      }

      if (null != maxWidget) {
        context.write(maxWidget, NullWritable.get());
      }
    }
  }

  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(MaxWidgetId.class);

    job.setMapperClass(MaxWidgetMapper.class);
    job.setReducerClass(MaxWidgetReducer.class);

    FileInputFormat.addInputPath(job, new Path("widgets"));
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Widget.class);

    job.setOutputKeyClass(Widget.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1; // Błąd
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new MaxWidgetId(), args);
    System.exit(ret);
  }
}
