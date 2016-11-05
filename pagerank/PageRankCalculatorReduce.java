package pagerank;
/*
 * Author:Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * reducer takes page and links to it and calculates pagerank based on formula
 * P(A) = (1-d) + d*(P(T1)/C(T1) + P(T2)/C(T2) + ... ) where T1, T2, ...Tn - pages pointing to A
 * C(A) - nr of links going out of page A
 * as it is iterative we must store original links structure
 * @author ktarnows
 *
 */
public class PageRankCalculatorReduce extends Reducer<Text, Text, Text, Text>{
	

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException,  InterruptedException {
			//System.out.println("PR Calculate Reduce");
	//damping factor = 0.85
	double d = 0.85;
	//page rank
	float pr = 0;
	
	//save list of links
	String links = "";
	
	//iterate through values , that is, pages pointing to the current page
	

    for ( Text val  : values) {
    	String value = val.toString();
    	
    	if(value.startsWith("-original-")){
    		links = value.substring(10);
    	}
    	else{
    	
    	//parse
    	String[] parsed = value.split("\\t");
    	//extract current page rank
    	float c_pr = Float.valueOf(parsed[1]);
    	
    	//extract nr of links
    	int linkNr = Integer.valueOf(parsed[2]);
    	
    	//recalculate the page rank
    	pr += c_pr / linkNr;
    	}
    	
    }
    
    //recalculate by considering damping factor
    double pr_final = (1-d) + d*pr;
    
	
	//write out
    context.write(key, new Text(pr_final+"\t"+links));
	}
	

}
