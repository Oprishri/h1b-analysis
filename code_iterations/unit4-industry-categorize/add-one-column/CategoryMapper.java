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
public class CategoryMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String line = value.toString().toLowerCase().trim(); // to lower_case
      String[] parts = line.split("\t");
     
      if ( parts != null && parts.length == 12
        && parts[0] != "" && parts[1] != "" && parts[2] != "" && parts[3] != ""
        && parts[4] != "" && parts[5] != "" && parts[6] != "" && parts[7] != ""
        && parts[8] != "" && parts[9] != "" && parts[10] != "" && parts[11] != ""
      ) {
         // drop quotes and trim spaces 
        String case_num =        parts[0].replace("\"", "").trim();
        String case_status =     parts[1].replace("\"", "").trim();
        String submission_date = parts[2].replace("\"", "").trim();
        String decision_date =   parts[3].replace("\"", "").trim();
        String employer_start  = parts[4].replace("\"", "").trim();
        String employer_end =    parts[5].replace("\"", "").trim();
        String e_n =   parts[6].replace("\"", "").trim(); // employer_name
        String employer_state =  parts[7].replace("\"", "").trim();
        String employer_city =   parts[8].replace("\"", "").trim();
        String j_t =       parts[9].replace("\"", "").trim(); // job_title
        String wage_rate =       parts[10].replace("\"", "").trim();
        String wage_unit =       parts[11].replace("\"", "").trim();
        
        String category = whichCategory(e_n, j_t);
        // reformat
        String newKey = case_num; // case_num
        String separator = "\t";
        String newValue =
                  case_status + separator // case_status
                + submission_date + separator  // submission_date 
                + decision_date + separator // decision_date
                + employer_start + separator  // employer_start
                + employer_end + separator // employer_end
                + e_n + separator  // employer_name
                + employer_state + separator  // employer_state
                + employer_city + separator  // employer_city
                + j_t + separator // job_title
                + wage_rate + separator // wage_rate
                + "year" + separator // wage_unit
                + category;      // category
        context.write(new Text(newKey), new Text(newValue));
      }
    }

    private String whichCategory(String e_n, String j_t) {
      String category;
      if ( e_n.contains("management") || j_t.contains("management") 
      ) {
        return "0";
      } else if ( 
        e_n.contains("architects") || e_n.contains("design") || e_n.contains("construction")
        || j_t.contains("architects") || j_t.contains("design") || j_t.contains("construction")
      ) {
        return "1";
      } else if (
        e_n.contains("school") || e_n.contains("university") || e_n.contains("research") || e_n.contains("children") || e_n.contains("science") ||
        j_t.contains("school") || j_t.contains("university") || j_t.contains("research") || j_t.contains("children") || j_t.contains("science")  
      ) {
        return "2";
      } else if (
        e_n.contains("medical") || e_n.contains("health") || e_n.contains("hospital") || e_n.contains("care") || e_n.contains("dental") || e_n.contains("healthcare") || e_n.contains("clinic") || e_n.contains("life") || e_n.contains("medicine") || e_n.contains("pharmacy") || e_n.contains("mental") || e_n.contains("pharmaceuticals") ||
        j_t.contains("medical") || j_t.contains("health") || j_t.contains("hospital") || j_t.contains("care") || j_t.contains("dental") || j_t.contains("healthcare") || j_t.contains("clinic") || j_t.contains("life") || j_t.contains("medicine") || j_t.contains("pharmacy") || j_t.contains("mental") || j_t.contains("pharmaceuticals")
      ) {
        return "3";
      } else if (
        e_n.contains("information") || e_n.contains("software") || e_n.contains("technologies") || e_n.contains("technology") || e_n.contains("engineering") || e_n.contains("pc") || e_n.contains("development") || e_n.contains("data") || e_n.contains("tech")
        || e_n.contains("technical") || e_n.contains("electronics") ||
        j_t.contains("information") || j_t.contains("software") || j_t.contains("technologies") || j_t.contains("technology") || j_t.contains("engineering") || j_t.contains("pc") || j_t.contains("development") || j_t.contains("data") || j_t.contains("tech")
        || j_t.contains("technical") || j_t.contains("electronics")
      ) {
        return "4";
      } else if (
        e_n.contains("consulting") || e_n.contains("business") || e_n.contains("financial") || e_n.contains("insurance") || e_n.contains("law") || e_n.contains("trading") || e_n.contains("bank") || e_n.contains("communications") || e_n.contains("consultants")
        || e_n.contains("mortgage") || e_n.contains("partners") || e_n.contains("investments") || e_n.contains("holdings") || e_n.contains("investment") ||
        j_t.contains("consulting") || j_t.contains("business") || j_t.contains("financial") || j_t.contains("insurance") || j_t.contains("law") || j_t.contains("trading") || j_t.contains("bank") || j_t.contains("communications") || j_t.contains("consultants")
        || j_t.contains("mortgage") || j_t.contains("partners") || j_t.contains("investments") || j_t.contains("holdings") || j_t.contains("investment")
      ) {
        return "5";
      } else if (
        e_n.contains("entertainment") || j_t.contains("entertainment")
      ) {
        return "6";
      } else {
        return "7";
      }
    }
}
