package tfidf1;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IdfMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	static int fileCount=0;
	public void map(LongWritable key,Text value,Context context) throws InterruptedException, IOException{
	    
		String s=value.toString();
		String[] words=s.split(":");
		String[] term=words[0].split("-");
		System.err.println("The word is "+words[0]+" - "+words[1]);
		System.err.println("The term length is " +term.length);
		String tf=words[1];
		String doc_id=term[0];
		String fterm=term[1];
		String value1=doc_id+"-"+tf;
		context.write(new Text(fterm),new Text(value1));
		
	}//map
	
	/*public void cleanup(Context context) throws IOException, InterruptedException{
		String cnt=Integer.toString(fileCount);
		context.write(new Text("TotalCount"),new Text(cnt));
	}//cleanup
	*/

}
