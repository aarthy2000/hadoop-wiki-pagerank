/*
 * author Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
package pagerank;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main class
 * invokes three jon classes to handle three tasks:
 * 1)Build Graph
 * 2)Iteratively calculate PageRank
 * 3)Sort the final list
 * @author ktarnows
 *
 */
public class Driver extends Configured implements Tool{
	
	private static NumberFormat f = new DecimalFormat("00");
	
	public static void main(String[] args)throws Exception{
		int res  = ToolRunner .run( new Configuration(), new Driver(), args);
	      System .exit(res);
	}
	
	public int run(String[] args) throws Exception{
		//create linkage graph, run the link graph generator job
		boolean run = graphBuilderJob("input", "output/rank/it00");
		if (!run) return 1;
		
		String output = null;
		
		//calculate PageRank for 10 iterations, run the job
		for(int it=0; it<10; it++){
			System.out.println("Iteration:"+it);
			//update input path
			String input = "output/rank/it"+f.format(it);
			// update output path
			output = "output/rank/it"+f.format(it+1);
			
			run = pageRankCalculatorJob(input, output);
			
			if (!run) return 1;
		}
		
		//run the cleanup pass
		run = pageRankSorterJob(output, "result");
		
		if(!run) return 1;
		return 0;
		
	}
	
	public boolean graphBuilderJob(String inPath, String outPath) throws Exception{
		
		Configuration conf = getConf();
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.get(conf);
		//store the number of pages in the context to calculate initial page rank
		conf.set("pageNr",Integer.toString(countPages(fs,inPath)));
		Job job  = Job .getInstance(getConf(), " graphGenerator ");
		job.setJarByClass( this .getClass());
		
		job.setOutputKeyClass( Text .class);
	    job.setOutputValueClass( Text .class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job,  new Path(inPath));
	    FileOutputFormat.setOutputPath(job,  new Path(outPath));
	    job.setMapperClass( GraphBuilderMap .class);
	    job.setReducerClass( GraphBuilderReduce .class);
	    
	    return job.waitForCompletion( true);
		
	}
	
	/**
	 * Job for calculating page rank
	 * @param inPath
	 * @param outPath
	 * @return
	 * @throws Exception
	 */
	
	private boolean pageRankCalculatorJob(String inPath, String outPath) throws Exception{
		Job job  = Job .getInstance(getConf(), " pageRankCalculator ");
		job.setJarByClass( this .getClass());
		
		job.setOutputKeyClass( Text .class);
	    job.setOutputValueClass( Text .class);
	    job.setMapperClass( PageRankCalculatorMap .class);
	    
	    FileInputFormat.addInputPath(job,  new Path(inPath));
	    FileOutputFormat.setOutputPath(job,  new Path(outPath));  
	    job.setReducerClass(PageRankCalculatorReduce .class);
	    
	    return job.waitForCompletion( true);   		
	}
	/**
	 * Cleanup and Sorting
	 * @param inPath
	 * @param outPath
	 * @return
	 * @throws Exception
	 */
	private boolean pageRankSorterJob(String inPath, String outPath) throws Exception{
		Job job  = Job .getInstance(getConf(), " pageRankSorter");
		job.setJarByClass( this .getClass());
		
		job.setOutputKeyClass( Text.class);
	    job.setOutputValueClass( Text .class);
	    job.setMapperClass(PageRankSorterMapper.class);
	    job.setReducerClass(PageRankSorterReduce.class);
	    //no. of reduce tasks equal 1 to enforce global sorting
	    job.setNumReduceTasks(1);
	    
	    FileInputFormat.addInputPath(job,  new Path(inPath));
	    FileOutputFormat.setOutputPath(job,  new Path(outPath)); 
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setSortComparatorClass(ReverseComparator.class);
	    
	    return job.waitForCompletion(true);
		
	}
	//define comparator for decreasing order
	/*
	   public static class DecreasingComparator extends WritableComparator{
	  	 public int compare(WritableComparable s1, WritableComparable s2){
	  		 LongWritable d1 = (LongWritable) s1;
	  		 LongWritable d2 = (LongWritable) s2;
	  	return -1*d1.compareTo(d2);	 
	  	 }
	   }
	*/
	
	 //define comparator for decreasing order
	   public static class DecreasingComparatorDouble extends WritableComparator{
	  	 public int compare(String s1, String s2){
	  		 Double d1 = Double.parseDouble(s1);
	  		 Double d2 = Double.parseDouble(s2);
	  	return -1*Double.compare(d1, d2);	 
	  	 }
	   }
	   
	 //define comparator for decreasing order
	   public static class DecreasingComparatorFloat extends WritableComparator{
		   protected DecreasingComparatorFloat(){
			   super(Text.class, true);
		   }
		   public int compare(String s1, String s2){
			   Float d1 = Float.valueOf(s1);
			   Float d2 = Float.valueOf(s2);
	  	return -1*Float.compare(d1,d2);	 
	  	 }
	   }
	   
	   public static class DecreasingComparatorLW extends WritableComparator{
	   protected DecreasingComparatorLW(){
		   super(LongWritable.class,true);
	   }
	   @Override
	   public int compare(WritableComparable o1, WritableComparable o2){
		   LongWritable k1 = (LongWritable) o1;
		   LongWritable k2 = (LongWritable) o2;
		   int cmp = k1.compareTo(k2);
		   return -1*cmp;
	   }
	   }
	   
	   static class ReverseComparator extends WritableComparator {
	        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

	        public ReverseComparator() {
	            super(Text.class);
	        }

	        @Override
	        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	            return (-1)* TEXT_COMPARATOR
				        .compare(b1, s1, l1, b2, s2, l2);
	        }

	        @Override
	        public int compare(WritableComparable a, WritableComparable b) {
	            if (a instanceof Text && b instanceof Text) {
	                return (-1)*(((Text) a)
	                        .compareTo((Text) b));
	            }
	            return super.compare(a, b);
	        }
	    }
	   

	
	/**
	 * funcion to count nr of pages in a file
	 * count occurences of <title>
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	private int countPages(FileSystem fs, String inPath) throws IllegalArgumentException, IOException{
		int count = 0;
		FileStatus[] status = fs.listStatus(new Path(inPath));
		 for(int i=0; i<status.length;i++){
			 BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			 String line;
			 line = br.readLine();
			 System.out.println("Line"+line);
			 while(line != null){
				 if(line.contains("<title>")){
					 count++;
					 System.out.println("Count"+count);
				 }
				 line = br.readLine();
			 }
			 br.close();
		 }
		 
		return count;
	}
	
	/**
	 * funtion to count nr of pages in a file
	 * count occurences of <title>
	 * @throws FileNotFoundException 
	 * not used
	 */
	private int countPages2(String pathStr) throws FileNotFoundException{
		String filename = new File(pathStr).getName();
		File file = new File(filename);
		int count = 0;
		Scanner scanner = new Scanner(file);
		while (scanner.hasNextLine()){
			
			String nextToken = scanner.next();
			if(nextToken.equals("<title>"))
				count++;
		}
		scanner.close();
		return count;
	}
	
	
}