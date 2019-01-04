import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataCleanReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        for (Text value : values) {
            String line = value.toString().toLowerCase();
            String[] column = line.split("¢");
            line = key.toString().toLowerCase();
            boolean writable = true;

            if (column[0].equals("certified-withdrawn"))
            {
                column[0] = "certified";
            }
            else if (column[0].equals("withdrawn"))
            {
                writable = false;
            }
            double wage = Double.parseDouble(numberForm(column[9]));
            if (column.length == 11)
            {
                if (column[10].equals("bi-weekly"))
                {
                    column[9] = String.valueOf(wage*25);
                }
                else if(column[10].equals("hour"))
                {
                    column[9] = String.valueOf(wage*2000);
                }
                else if(column[10].equals("week"))
                {
                    column[9] = String.valueOf(wage*50);
                }
                else if(column[10].equals("month"))
                {
                    column[9] = String.valueOf(wage*12);
                }
                else if(column[10].equals("year"))
                {
                    column[9] = String.valueOf(wage);
                }
            }
            else{
                //the last column is missing, length is 10 at this time
                //column[9] = String.valueOf(wage);
                //column = Arrays.copyOf(column, column.length + 1);
                //column[10] = "";
                writable = false;
            }
            for (int i=0;i< column.length;i++)
            {
                if (column[i].length() == 0)
                {
                    writable = false;
                }
                else if (i == 1||i==2||i==3||i==4)
                {
                    column[i] = normalizeDate(column[i]);
                }
                else if (i == 5||i==7||i==8)
                {
                    column[i] = column[i].replace("\"","");
                }
                line = line + "\t" + column[i];
            }
	        
            if (writable)
            {
                context.write(new Text(line), new Text());
            }
            
        }

    }
    public String numberForm(String s)
    {
        String[] num = s.split("\\.");
        num[0]=num[0].replace(",","");
        num[0]=num[0].replace("\"","");
        num[1]=num[1].replace("\"","");
        num[1]=num[1].replace(",","");
        return num[0]+"."+num[1];
    }
    public String normalizeDate(String date)
    {
        String[] d = date.split("/");
        if (d[1].length() == 1) {
            d[1] = "0" + d[1];
        }
        if (d[2].length() == 1) {
            d[2] = "0" + d[2];
        }
        return d[0]+d[1]+d[2];
    }
}
