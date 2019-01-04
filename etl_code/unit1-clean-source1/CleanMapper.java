import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
    0 YYYYYY "SUBMITTED_DATE" 2
    1 YYYY "CASE_NO" 0
    2 YYYY "NAME" 4
    3 "ADDRESS"
    4 "ADDRESS2"
    5 YYYY "CITY" 6
    6 YYYY "STATE" 5
    7 "POSTAL_CODE"
    8 "NBR_IMMIGRANTS"
    9 YYYYY "BEGIN_DATE" 2
    10 YYYYYYY "END_DATE"
    11 YYYYY "JOB_TITLE" 7
    12 YYYYY "DOL_DECISION_DATE" 3
    13 Y "CERTIFIED_BEGIN_DATE"
    14 Y "CERTIFIED_END_DATE"
    15 "JOB_CODE"
    16 YYYYYY "APPROVAL_STATUS" 1
    17 YYYYYY "WAGE_RATE_1" 8
    18 YYYYYY "RATE_PER_1" 9
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
  /***
 case_num string,
 case_status string,
 submission_date string,
 decision_date string,
 employer_start string,
 employer_end string,
 employer_name string,
 employer_state string,
 employer_city string,
 job_title string,
 wage_rate string,
 wage_unit string
 **/
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = value.toString().toLowerCase().trim().replace("\t", ""); // to lower_case
      String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      // for normal key value pairs
      // drop some columns that is not relevant
      // drop some rows that missing critical columns
      String case_status;
      if (parts != null && parts.length == 37) {
        case_status = parts[16].replace("\"", "").trim();
      } else {
        return;
      }
      if (case_status.equals("pending")) {
        return;
      }
      if ( parts != null && parts.length == 37
        && parts[1] != "" && parts[16] != "" && parts[0] != "" && parts[12] != ""
        && parts[9] != "" && parts[10] != "" && parts[2] != "" && parts[6] != ""
        && parts[5] != "" && parts[11] != "" && parts[17] != "" && parts[18] != ""
      ) {
        //drop comma
        //    划重点!!!! replace之后产生新的string 要用 String 接住,原来的String不会变,因为immutable啊!
          // for (int i = 0; i < parts.length; i++) {
          //   parts[i] = parts[i].replace(",", "");
          // }
        // drop quotes and trim spaces 
        String case_num =        parts[1].replace("\"", "").trim();
        String submission_date = parts[0].replace("\"", "").trim();
        String decision_date =  parts[12].replace("\"", "").trim();
        String employer_start  = parts[9].replace("\"", "").trim();
        String employer_end =   parts[10].replace("\"", "").trim();
        String employer_name =   parts[2].replace("\"", "").trim();
        String employer_state =  parts[6].replace("\"", "").trim();
        String employer_city =   parts[5].replace("\"", "").trim();
        String job_title =      parts[11].replace("\"", "").trim();
        String wage_rate =      parts[17].replace("\"", "").trim();
        String wage_unit =      parts[18].replace("\"", "").trim();
        
        // reformat
        if (submission_date.length() < 8) {
          return;
        }
        submission_date = submission_date.replace("-", "").substring(0, 8);
        decision_date = dateFormat(decision_date);
        employer_start = dateFormat(employer_start);
        employer_end = dateFormat(employer_end);
        if (decision_date.equals("error") || employer_start.equals("error") || employer_end.equals("error")) {
          return;
        }
        wage_rate = wageFormat(wage_rate, wage_unit);
        String seperator = "\t";
        String newKey = case_num; // case_num
        String newValue =
                  case_status + seperator // case_status
                + submission_date + seperator  // submission_date 
                + decision_date + seperator // decision_date
                + employer_start + seperator  // employer_start
                + employer_end + seperator // employer_end
                + employer_name + seperator  // employer_name
                + employer_state + seperator  // employer_state
                + employer_city + seperator  // employer_city
                + job_title + seperator // job_title
                + wage_rate + seperator // wage_rate
                + "year";      // wage_unit
        context.write(new Text(newKey), new Text(newValue));
      }
    }

  // 猜测 month前面有 " 符号 , parse不进去 
  private String dateFormat(String date) {
    // private String dateFormat(String date){
      String[] dateArray = date.split("/");
      if (dateArray.length == 3) {
        String month = dateArray[0].trim();
        String day = dateArray[1].trim();
        String year = dateArray[2].trim();
        // month padding
        if(year.length() != 4) {
          return "error";
        }

        if (month.length() == 1) {
          month = "0" + month;
        } else if (month.length() == 2) {
          month = month;
        } else {
          return "error";
        }
        
        if (day.length() == 1) {
          day = "0" + day;
        } else if (day.length() == 2) {
          day = day;
        } else {
          return "01";
        }
        return year + month + day;
      } else {
        return "error";
      }
  }

  private String wageFormat(String wage, String wageUnit) {
    int wageInt = Double.valueOf(wage.trim()).intValue();
    switch (wageUnit.trim()) {
      case "2 weeks": wageInt *= 25;
          break;
      case "hour": wageInt *= 2000;
          break;
      case "month": wageInt *= 12;
          break;
      case "week": wageInt *= 50;
          break;
      default:
          break;
    }
    return String.valueOf(wageInt);
  }
  
}
