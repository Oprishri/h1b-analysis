import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
    0 Y "SUBMITTED_DATE"
    1 Y "CASE_NO"
    2 Y "NAME"
    3 "ADDRESS"
    4 "ADDRESS2"
    5 Y "CITY"
    6 Y "STATE"
    7 "POSTAL_CODE"
    8 Y "NBR_IMMIGRANTS"
    9 Y "BEGIN_DATE"
    10 Y "END_DATE"
    11 Y "JOB_TITLE"
    12 "DOL_DECISION_DATE"
    13 Y "CERTIFIED_BEGIN_DATE"
    14 Y "CERTIFIED_END_DATE"
    15 "JOB_CODE"
    16 Y "APPROVAL_STATUS"
    17 "WAGE_RATE_1"
    "RATE_PER_1"
    "MAX_RATE_1"
    "PART_TIME_1"
    "CITY_1"
    "STATE_1"
    "PREVAILING_WAGE_1"
    "WAGE_SOURCE_1"
    "YR_SOURCE_PUB_1"
    "OTHER_WAGE_SOURCE_1"
    "WAGE_RATE_2"
    "RATE_PER_2"
    "MAX_RATE_2"
    "PART_TIME_2"
    "CITY_2"
    "STATE_2"
    "PREVAILING_WAGE_2"
    "WAGE_SOURCE_2"
    "YR_SOURCE_PUB_2"
    "OTHER_WAGE_SOURCE_2"
 **/
public class ProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = value.toString();
      String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      Integer[] Index = {0, 1, 2, 5, 6, 8, 9, 10, 11, 13, 14, 16};
      String[] ColumnName = {"SUBMITTED_DATE", "CASE_NO", "NAME", "CITY", "STATE", 
      "NBR_IMMIGRANTS", "BEGIN_DATE", "END_DATE", "JOB_TITLE", "CERTIFIED_BEGIN_DATE", 
      "CERTIFIED_END_DATE", "APPROVAL_STATUS"};

      if (parts != null && parts.length == 37) {
          for (int i = 0; i < Index.length; i++) {
            context.write(new Text(ColumnName[i]), new IntWritable(parts[Index[i]].length()));
          }
      }
  }
}
