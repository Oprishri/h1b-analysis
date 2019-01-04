import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;

public class ProfileDriver {
  public static void main(String[] args) throws Exception{
    if (args.length != 2) {
      System.err.println("Usage: ProfileDriver <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(ProfileDriver.class);
    job.setJobName("Profile Data");
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(ProfileMapper.class);
    job.setReducerClass(ProfileReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
