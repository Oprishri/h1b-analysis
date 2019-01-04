import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      String keyString = key.toString();
      int totalCounts = 0;
      String separator = "\t";
      for (IntWritable value : values) {
        totalCounts += value.get();
      }
      context.write(new Text(keyString.trim() + separator + String.valueOf(totalCounts).trim()), new Text());
  }
}
