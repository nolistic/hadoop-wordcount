/**
 * Heather Nolis
 * Data Science
 * SeattleU
 * HW1
 */

package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ProcessLogs {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(ProcessLogs.class);
    job.setJobName("Process Logs");
    
    job.setMapperClass(LogFileMapper.class);
    job.setReducerClass(SumReducer.class);
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

 
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    

    


    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
