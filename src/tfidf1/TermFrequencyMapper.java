package tfidf1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

//import TFIDF.TermFrequencyDriver.counts;


public class TermFrequencyMapper extends Mapper<FileInputWritable,Text,Text,Text>{
	
	public Text doc_id_word = new Text();
	final static IntWritable one = new IntWritable(1);
	private String fName ;
	//public static enum counts {DOCUMENTS, TERMS};
	public static long term_counter;
	public static long doc_counter;
	//Path path =null;
	public void setup(Context context) throws IOException{
		CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
		System.err.println("The current path is "+inputSplit.getLength() + inputSplit.getPaths());
		for(Path path : inputSplit.getPaths()) {
			
			String fileName = path.getName();
			System.err.println("Only file name" +fileName);
			fName = fileName.substring(0, fileName.lastIndexOf("."));
		}
		
			
		
	}
	
	public void map(FileInputWritable key, Text content, Context context ) throws IOException, InterruptedException{
		
		String[] word = content.toString().split(" ");
		
		
		int count = word.length;
		
		for (int i = 0; i < word.length; i++) {
			
			doc_id_word.set(new Text(key.fileName+"-"+word[i].toString()));
			//count++;
			System.err.println("The key in the loop "+doc_id_word.toString());
			//context.getCounter(counts.TERMS).increment(1);
			context.write(doc_id_word, new Text(one+"-"+count));
			//context.write(new Text("TermCount"), new IntWritable(count));
			term_counter ++;
			
		}
		//System.err.println("The terms count is "+context.getCounter(counts.TERMS).getValue());
		
		//context.getCounter(TermFrequencyDriver.counts.DOCUMENTS).increment(1);
		doc_counter ++;	
		//counts.DOCUMENTS = 
		             
		             System.err.println("The term_counter is " + term_counter);
		             System.err.println("The doc_counter is "+ doc_counter);
		
		//System.err.println("The terms count is "+context.getCounter(counts.DOCUMENTS).getValue());
	
	}//map
	
	public void cleanup(Context context) throws IOException, InterruptedException{
	      org.apache.hadoop.conf.Configuration c=context.getConfiguration();
	       String d=Long.toString(doc_counter);
	       System.out.println("In TF Mapper::"+d);
	       c.set("DocCount",d);
	       context.write(new Text("FileCount-fc"),new Text(d));
	      
	}//cleanup
	
	
	
}
