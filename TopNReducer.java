package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text>{
	
	private int N = 10; // by default
	private SortedMap<Integer, String> topNItems = new TreeMap<Integer, String>();
	// used to store movieId-movieTitle pairs
	private Map<String, String> lookup = new HashMap<String, String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10);
		
		// read movieId-movieTitle pairs to map
		try {
			// Read words and put into positive set
			File titleFile = new File("movie_titles.txt");
			BufferedReader reader = new BufferedReader(new FileReader(titleFile));
			String str;
			while((str = reader.readLine()) != null){
				String[] items = new String[3];
				
				int firstCutOff = str.indexOf(",");
			    items[0] = str.substring(0, firstCutOff);
			
			    int secondCutOff = str.indexOf(",", firstCutOff+1);
			    items[1] = str.substring(firstCutOff+1, secondCutOff);
			    
			    items[2] = str.substring(secondCutOff+1);

				lookup.put(items[0], items[2]);
			}
			reader.close();
		}catch(IOException e){
			throw new RuntimeException("Illegal file reading " + e.getMessage());
		}
		
	}
	
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		for (Text value : values) {
			// value is as (movieId,count)
			String[] item = value.toString().trim().split(",");
			String movieId = item[0];
			int count = Integer.parseInt(item[1]);
			
			// put onto list
			topNItems.put(count, movieId);
			// adjusting to keep the size as N
			if(topNItems.size() > N){
				topNItems.remove(topNItems.firstKey());
			}
		}
		
		List<Integer> list = new ArrayList<Integer>(topNItems.keySet());
		for(int i = list.size()-1; i >= 0; i--){
			// get the movieId and title
			int count = list.get(i);
			String movieId = topNItems.get(count);
			String movieTitle = lookup.get(movieId);
			
			context.write(new IntWritable(count), new Text(movieTitle));
		}
	}

}
