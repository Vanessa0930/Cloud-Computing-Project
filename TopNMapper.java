package stubs;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	private int N = 10; // default value
	// use TreeMap to store items based on the nature order of rating ==> RBT
	private SortedMap<Integer, String> topNItems = new TreeMap<Integer, String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10); // default value of N is 10
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	         throws IOException, InterruptedException{
		
		String[] items = value.toString().split(" ");
		String movieId = items[0];
		int count = Integer.parseInt(items[1]);
		// use compositeValue to remember movieId and rating inside this map cluster
		String compositeValue = new String(movieId + "," + count);
		
		// put onto topNItems list
		topNItems.put(count, compositeValue);
		if(topNItems.size() > N){
			// remove the entry with smallest count
			topNItems.remove(topNItems.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for(String str : topNItems.values()){
			context.write(NullWritable.get(), new Text(str));
		}
	}

}
