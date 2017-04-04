package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNDriver extends Configured implements Tool{

	
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf());
		
		// set up value of N 
		int N = Integer.parseInt(args[0]);
		job.getConfiguration().setInt("N", N);
		
		job.setJarByClass(TopNDriver.class);
		job.setJobName("Top N List");
		
		// args[0] is parameter N
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// setup mapper and reducer
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setNumReduceTasks(1);
		
		// setup output format
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
	    int exitCode = ToolRunner.run(new Configuration(), new TopNDriver(), args);
	    System.exit(exitCode);
	  }

}
