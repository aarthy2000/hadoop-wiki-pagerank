package pagerank;
/*
 * Author:Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankSorterMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	/**
	 * takes input:
	 * page pageRank links
	 * outputs:
	 * rank page
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] parsed = value.toString().split("\\t");
		
		//float pageRank = Float.parseFloat(parsed[1]);
		String pageRank = parsed[1];
		String page = parsed[0];
		
		//Output with rank as a key, by default sorted by key
		context.write(new Text(pageRank), new Text(page));
	}
	

}
