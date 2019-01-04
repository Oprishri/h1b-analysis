import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int countmax = 0;
        //int maxline;
        int countmin = 1000;
        //int minline;

        for (Text value : values) {
            String line = value.toString();
            countmax = Math.max(line.length(),countmax);
            countmin = Math.min(line.length(),countmin);
        }
        String line = String.valueOf(countmax)+" and "+String.valueOf(countmin);
        context.write(key, new Text(line));
    }
}