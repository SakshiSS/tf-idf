package tfidf1;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import javax.print.attribute.standard.JobName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Reducer;

//import TFIDF.TermFrequencyDriver.counts;

public class TermFrequencyReducer extends Reducer<Text ,Text,Text,Text>{
	
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		long term_count = 0;
	if(!key.toString().equals("FileCount-fc")){
			
	    System.out.println("Entry Level !FileCount");	
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()){
			String[] val = itr.next().toString().split("-");
			sum=sum + Integer.parseInt(val[0]);
				
			term_count = Long.parseLong(val[1]);
		}
	}//if
	else{
		System.out.println("Key is equal to  entry level"+key.toString());
	}//else
		
		
		long final_count =  term_count;
		//Configuration conf = context.getConfiguration();
		//Job job = Job.getInstance(conf);
		//Counters counters = job.getCounters();
		//long t_count = context.getCounter(counts.TERMS).getValue();
		
		System.err.println("The number of terms at reducer = "+final_count);	
		//System.err.println("The number of terms at reducer from Counters = "+t_count);
		DecimalFormat df = new DecimalFormat("###.####");
		if(!key.toString().equals("FileCount-fc")){
			System.out.println("Key is !Equal FileCount exit");
		  context.write(key,new Text((df.format(((double)sum/(double)final_count)))));
		}//if
		else{
			Iterator<Text> itr = values.iterator();
			itr.hasNext();
			context.write(key,itr.next());
		    System.out.println("Key is equal to fileCount exit");
			
				
		}//else
		//context.write(key,new Text((df.format(((double)sum/(double)final_count)))));
	}

	

}
