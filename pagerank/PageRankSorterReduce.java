package pagerank;
/*
 * Author:Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Does the sorting
 * output: page score
 * @author ktarnows
 *
 */
public class PageRankSorterReduce extends Reducer<Text , Text ,  Text  ,  Text> {
    @Override 
    public void reduce( Text score,  Iterable<Text> pages,  Context context)
       throws IOException,  InterruptedException {

       for (Text page: pages){
      	context.write(page, score);
      	 //System.out.println("Wrote:"+file.toString()+"Value: "+score);
       }
       } 
    }