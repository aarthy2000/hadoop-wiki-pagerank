package pagerank;
/*
 * Author:Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * the input is Text in form
 * title initial_rank links_list
 * intermediate results (Map output) would be
 * titleOfPagePointed title rank links_number 
 * also we have to store the original links 
 * @author ktarnows
 *
 */
public class PageRankCalculatorMap extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws  IOException,  InterruptedException {
		//System.out.println("PR Calculate Mapper");
		String parsedValue[] = value.toString().split("\\t");
		//System.out.println(parsedValue.length);
		
		String title = parsedValue[0];
		String links;
		if(parsedValue.length > 2){
		links = parsedValue[2];
		String[] linksArray = links.split(",");
		int nrLinks = linksArray.length;
		String newValue = parsedValue[0] + "\t" + parsedValue[1] + "\t";
		for (String link: linksArray){
			
			context.write(new Text(link),new Text(newValue+nrLinks));
			//System.out.println("Wrote: "+link +" "+newValue+nrLinks);
		}
		}
		else{
			links = "";
		}

		context.write(new Text(title), new Text("-original-"+links));
		//System.out.println("Wrote: "+title +"-original-"+links);
	}

}
