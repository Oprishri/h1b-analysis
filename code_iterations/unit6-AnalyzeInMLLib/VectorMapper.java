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
import java.util.*;
/***
n  name            | dimension |range
00 case_status     |  1        |0,1,2
01 wage_rate       |  1
02 decisiontime    |  1
03 employtime      |  1
04 employer_state  |  56
05 occupation      |  8
 **/
public class VectorMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString();
            String[] column = line.split("\t");
            List<String> outkey = new ArrayList<String>();
            String key1;
            boolean writable = true;
            if(column[0].equals("certified"))
            {
                key1 = "1";
            }
            else if(column[0].equals("denied"))
            {
                key1 = "0";
            }
            else
            {
                key1 = "";
                writable = false;
            }
            String value1 = "";

            if (column.length == 9 && column[0].matches("(.{0,10})") == true) {
                outkey.add(column[0]);
                outkey.add(column[1]);
                if (Double.parseDouble(column[1])<0)
                {
                    writable = false;
                }
                outkey.add(column[2]);
                outkey.add(column[3]);
                int num = keyOfState(column[4]);
                List<String> temp = new ArrayList<String>(Collections.nCopies(56, "0"));
                temp.set(num-1,"1");
                outkey.addAll(temp);

                List<String> temp2 = new ArrayList<String>(Collections.nCopies(8, "0"));
                num = (int)Double.parseDouble(column[8]);
                temp2.set(num,"1");
                outkey.addAll(temp2);

                String t = "\t";
                for (int i=1;i<outkey.size();i++)
                {
                    key1 = key1 + t + outkey.get(i);
                }
                if (writable){
                    context.write(new Text(key1), new Text(value1));
                }
            }
        }

        public int keyOfState(String word)
        {
            if (word.equals("ak"))
            {
                return 1;
            }
            else if (word.equals("al"))
            {
                return 2;
            }
            else if (word.equals("ar"))
            {
                return 3;
            }
            else if (word.equals("az"))
            {
                return 4;
            }
            else if (word.equals("ct"))
            {
                return 5;
            }
            else if (word.equals("ca"))
            {
                return 6;
            }
            else if (word.equals("co"))
            {
                return 7;
            }
            else if (word.equals("dc"))
            {
                return 8;
            }else if (word.equals("de"))
            {
                return 9;
            }
            else if (word.equals("fl"))
            {
                return 10;
            }
            else if (word.equals("ga"))
            {
                return 11;
            }
            else if (word.equals("gu"))
            {
                return 12;
            }
            else if (word.equals("hi"))
            {
                return 13;
            }
            else if (word.equals("ia"))
            {
                return 14;
            }
            else if (word.equals("il"))
            {
                return 15;
            }
            else if (word.equals("in"))
            {
                return 16;
            }
            else if (word.equals("ia"))
            {
                return 17;
            }
            else if (word.equals("id"))
            {
                return 18;
            }
            else if (word.equals("ks"))
            {
                return 19;
            }
            else if (word.equals("ky"))
            {
                return 20;
            }
            else if (word.equals("ma"))
            {
                return 21;
            }
            else if (word.equals("me"))
            {
                return 22;
            }
            else if (word.equals("mo"))
            {
                return 23;
            }
            else if (word.equals("mi"))
            {
                return 24;
            }
            else if (word.equals("ms"))
            {
                return 25;
            }
            else if (word.equals("md"))
            {
                return 26;
            }else if (word.equals("mn"))
            {
                return 27;
            }else if (word.equals("mp"))
            {
                return 28;
            }else if (word.equals("mt"))
            {
                return 29;
            }
            else if (word.equals("nd"))
            {
                return 30;
            }
            else if (word.equals("nh"))
            {
                return 31;
            }
            else if (word.equals("nj"))
            {
                return 32;
            }
            else if (word.equals("nv"))
            {
                return 33;
            }
            else if (word.equals("nc"))
            {
                return 34;
            }
            else if (word.equals("ne"))
            {
                return 35;
            }
            else if (word.equals("nm"))
            {
                return 36;
            }
            else if (word.equals("ny"))
            {
                return 37;
            }
            else if (word.equals("ok"))
            {
                return 38;
            }
            else if (word.equals("oh"))
            {
                return 39;
            }
            else if (word.equals("or"))
            {
                return 40;
            }
            else if (word.equals("pr"))
            {
                return 41;
            }
            else if (word.equals("pa"))
            {
                return 42;
            }
            else if (word.equals("ri"))
            {
                return 43;
            }
            else if (word.equals("sc"))
            {
                return 44;
            }
            else if (word.equals("sd"))
            {
                return 45;
            }
            else if (word.equals("tn"))
            {
                return 46;
            }
            else if (word.equals("tx"))
            {
                return 47;
            }
            else if (word.equals("ut"))
            {
                return 48;
            }
            else if (word.equals("vt"))
            {
                return 49;
            }
            else if (word.equals("va"))
            {
                return 50;
            }
            else if (word.equals("vi"))
            {
                return 51;
            }
            else if (word.equals("wa"))
            {
                return 52;
            }
            else if (word.equals("wi"))
            {
                return 53;
            }
            else if (word.equals("wy"))
            {
                return 54;
            }
            else if (word.equals("wv"))
            {
                return 55;
            }
            else if (word.equals("fm"))
            {
                return 56;
            }
            return 1;
        }




}
