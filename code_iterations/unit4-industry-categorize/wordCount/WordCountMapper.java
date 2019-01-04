import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = value.toString().toLowerCase().trim().replace("\t", " ");
      String[] parts = line.split(" ");
      // check for each keyword
      for (String keyWord : parts) {
        context.write(new Text(keyWord.trim()), new IntWritable(1));
      }
  }
}
