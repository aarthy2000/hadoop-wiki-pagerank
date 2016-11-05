/*
 * Author:Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphBuilderReduce extends Reducer<Text, Text, Text, Text>{
	
	/**
	 * Initialize link graph with 1/N as initial PageRank value
	 * damping factor = 0.85, N - total pages in the corpus
	 * output is in form:
	 * title initialPageRank links 
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException,  InterruptedException {
		
		//System.out.println("Reduce");
		// get the number of pages in corpus through context
		String nrPagesStr = context.getConfiguration().get("pageNr");
		
		int nrPages = Integer.parseInt(nrPagesStr);
		//System.out.println("Nr of pages: "+ nrPages);
		
		double initialPageRank = ((double)1)/nrPages;
		//System.out.println("Initial page rank: "+initialPageRank);
		
		String initialPageRankStr =Double.toString(initialPageRank);
		String value = initialPageRankStr+"\t";
		
		boolean firstLink = true;

        for ( Text val  : values) {
        	if(!firstLink) value += ",";
        	value += val.toString();
        	firstLink = false;      
        }
        
        context.write(key, new Text(value));	
	}

}
