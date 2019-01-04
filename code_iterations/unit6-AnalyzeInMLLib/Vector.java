import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/***
00 case_status     | string 1
01 wage_rate       | double 1
02 decisiontime    | bigint 1
03 employtime      | bigint 1
04 employer_state  | string 55
05 employer_city   | string 
06 employer_name   | string 
07 job_title       | string 
08 occupation
 **/
public class Vector {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Vector <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Vector.class);
        job.setJobName("Vector");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(VectorMapper.class);
        job.setReducerClass(VectorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
