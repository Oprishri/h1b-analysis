import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/***
 00 case_num string,
 01 case_status string,
 02 submission_date string,
 03 decision_date string,
 04 employer_start string,
 05 employer_end string,
 06 employer_name string,
 07 employer_state string,
 08 employer_city string,
 09 job_title string,
 10 wage_rate string,
 11 wage_unit string
 **/
public class DataClean {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DataClean <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(DataClean.class);
        job.setJobName("Data Clean");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DataCleanMapper.class);
        job.setReducerClass(DataCleanReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
