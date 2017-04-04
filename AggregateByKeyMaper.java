package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateByKeyMaper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text movieId = new Text();
	private IntWritable rating = new IntWritable();
	
	
	public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException{
		
		String[] line = value.toString().trim().split(",");
		if( line.length != 3){
			return;
		}
		
		movieId.set(line[0]);
		int num = (int)(Double.parseDouble(line[2]));
		rating.set(new Integer(num));
		
		context.write(movieId, rating);

	}
	

}
