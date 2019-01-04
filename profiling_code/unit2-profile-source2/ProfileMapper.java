import java.io.IOException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProfileMapper extends Mapper<LongWritable, Text,IntWritable, Text>{

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        String line = value.toString();
        String[] column = line.split("¢");
        int k;
        String v;
        if (column.length == 12 && column[0].matches("(.{17,19})") == true) {
            k = 1;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 2;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 3;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 3;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 4;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 5;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 6;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 7;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 8;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 9;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 10;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
            k = 11;
            v = column[k];
            context.write(new IntWritable(k),new Text(v));
        }
    }


}