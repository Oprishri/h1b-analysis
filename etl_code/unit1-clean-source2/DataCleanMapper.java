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

public class DataCleanMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString();
            String[] column = line.split("\t");
            String key1;
            String value1 = "";

            if (column.length == 52 && column[0].matches("(.{35,40})") == true) {
                key1 = cleanString(column[0]);
                String t = "¢";
                value1 = value1 + cleanString(column[1]) + t + cleanString(column[2]) + t + cleanString(column[3])+ t + cleanString(column[5]) + t + cleanString(column[6]) + t + cleanString(column[7]) + t + cleanString(column[11]) + t  + cleanString(column[10]) + t + cleanString(column[21]) + t + cleanString(column[39]) + t + cleanString(column[41]);
                context.write(new Text(key1), new Text(value1));
            }
        }

        public String cleanString(String word)
        {
            String cleanword="";
            int i = 0;
            while (i < word.length())
            {
                i++;
                if (i < word.length())
                {
                    cleanword += word.charAt(i);
                }
                i++;
            }
            return cleanword;
        }




}
