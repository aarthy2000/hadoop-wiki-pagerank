/*
 * Author:Katarzyna Tarnowska
 * ktarnows@uncc.edu
 */
package pagerank;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * parses the file with pages
 * input is a Text object of single page
 * writes out pairs of each page and its outgoing links
 * @author ktarnows
 *
 */
public class GraphBuilderMap extends Mapper<LongWritable, Text, Text, Text>{
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws  IOException,  InterruptedException {
		
		String[] page = pageParser(value);
		//System.out.println("Current page: "+page[0]+" "+page[1]);
		String title = page[0];
		String body = page[1];
		
		
		ArrayList<String> links = bodyParser(body);
		if (!(links.isEmpty())){
		for(int j = 0; j < links.size(); j++){
			context.write(new Text(title),  new Text(links.get(j)));
			//System.out.println("Wrote: "+ title +" "+links.get(j));
		}
		}
	}
	/*
	 * function to parse an entry (page)
	 * return title and body
	 */
	public String[] pageParser(Text value) throws CharacterCodingException{
		String[] titleBody = new String[2];
		int titleEnd = -1;
		int bodyEnd = -1;
		int titleStart = value.find("<title>");
		if(titleStart != -1){
			titleEnd = value.find("</title>", titleStart);
		}
		int bodyStart = value.find("<text");
		if(bodyStart != -1){
		bodyStart = value.find(">", bodyStart);
		bodyEnd = value.find("</text>", bodyStart);
		}
		
		if (titleStart == -1 || titleEnd == -1 || bodyStart == -1 || bodyEnd == -1){
			return new String[]{"",""};
		}
		//discard the tags
		titleStart +=7;
		bodyStart +=1;
		
		//do the parsing
		titleBody[0] = Text.decode(value.getBytes(), titleStart, titleEnd-titleStart);
		//System.out.println("Title"+titleBody[0]);
		titleBody[1] = Text.decode(value.getBytes(), bodyStart, bodyEnd-bodyStart);
		//System.out.println("Body"+titleBody[1]);
		
		return titleBody;
	}
	
	/*
	 * function to parse body
	 * to find all the links
	 */
	
	private ArrayList<String> bodyParser(String body){
		ArrayList<String> links = new ArrayList<String>();
		int linkEnd, linkStartNested
		//, linkEndNested
		;
		int linkStart = body.indexOf("[[");
		while (linkStart >= 0){
			//System.out.println("LinkStart > 0");
			//omit the tag
			linkStart = linkStart +2;
			//handle situation of nested links
			linkEnd = body.indexOf("]]", linkStart);
			//System.out.println("linkEnd: "+linkEnd);
			linkStartNested = body.indexOf("[[", linkStart);
			//System.out.println("linkStartNested: "+linkStartNested);
			
			//case of nested
			if((linkStartNested != -1) && (linkStartNested < linkEnd)){
				break;
				/*
				System.out.println("Detected nest links");
				//handle nested link as a separate link
				linkEndNested = linkEnd;
				//omit tag
				linkStartNested = linkStartNested+2;
				linkEnd = body.indexOf("]]", linkEndNested);
				//add nested link
				if (linkEndNested == -1){
					break;
				}
				String newLink = body.substring(linkStartNested);
				newLink = newLink.substring(0,linkEndNested - linkStartNested);
				links.add(newLink);
				//add main link
				if (linkEnd == -1){
					break;
				}
				String newLink2 = body.substring(linkStart);
				newLink2 = newLink2.substring(0,linkEnd - linkStart);
				links.add(newLink2);
				*/
			}else
			//the normal case
			{
			if (linkEnd == -1){
				break;
			}
			//the normal case
			String newLink = body.substring(linkStart);
			newLink = newLink.substring(0, linkEnd - linkStart);
			links.add(newLink);
			//System.out.println(newLink);
			linkStart = body.indexOf("[[", linkEnd+1);
			}
			
			
		}
		return links;
		
	}
		

}
