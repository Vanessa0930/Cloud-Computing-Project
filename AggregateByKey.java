package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateByKey extends Configured implements Tool{

	
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Usage: " + this.getClass().getName() + " <input dir> <output dir>\n");
			return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(AggregateByKey.class);
		job.setJobName("Aggregate By Key");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// setup mapper, reducer and combiner
		job.setMapperClass(AggregateByKeyMaper.class);
		job.setReducerClass(AggregateByKeyReducer.class);
		job.setCombinerClass(AggregateByKeyReducer.class);
		
		// set key and vlue class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
	    int exitCode = ToolRunner.run(new Configuration(), new AggregateByKey(), args);
	    System.exit(exitCode);
	  }

}
