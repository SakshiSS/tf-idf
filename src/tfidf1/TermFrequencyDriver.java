package tfidf1;

import java.io.InputStream;
import java.nio.file.FileSystem;

import javax.sound.midi.Sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TermFrequencyDriver extends Configured implements Tool {
	
	public static enum counts {
		DOCUMENTS, TERMS
	
	};
	
	public static void main(String[] args) throws Exception{
		int rc= ToolRunner.run(new TermFrequencyDriver(), args);
		System.exit(rc);
		
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator ", ":");
		Job job_tf = Job.getInstance(conf);
		job_tf.setJobName("TF");
		
		job_tf.setJarByClass(TermFrequencyDriver.class);
		job_tf.setInputFormatClass(FileContentInputFormat.class);
		conf.set("mapreduce.input.fileinputformat.split.maxsize","268435456");
		//job.setOutputFormatClass(.class);
		Path path = new Path(args[0]);
		org.apache.hadoop.fs.FileSystem fs =  path.getFileSystem(conf);
		FileStatus[] fileStatus = fs.listStatus(path);
		boolean job_idf_complete = false;
//		for (FileStatus fStatus : fileStatus) {
//			System.err.println("The path in driver "+fStatus.getPath().toString());
//			FileInputFormat.addInputPath(job_tf, fStatus.getPath());
//		}
		FileInputFormat.addInputPath(job_tf, new Path(args[0]));
		org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job_tf,new Path(args[1]));
		job_tf.setMapOutputKeyClass(Text.class);
		job_tf.setMapOutputValueClass(Text.class);
		job_tf.setOutputKeyClass(Text.class);
		job_tf.setOutputValueClass(Text.class);	
		
		
		job_tf.setMapperClass(TermFrequencyMapper.class);
		job_tf.setReducerClass(TermFrequencyReducer.class);	
		
		
		//Counters counters = job.getCounters();
		boolean job_tf_complete = job_tf.waitForCompletion(true);
		//return(job_tf.waitForCompletion(true)? 0: 1);
		
		
		//next map-reduce job for calculating idf
		if(job_tf_complete ){
			
		Configuration conf2 = new Configuration();
		Job job_idf = Job.getInstance(conf2);
		
		
		job_idf.setJobName("IDF");
		job_idf.setMapperClass(IdfMapper.class);
		job_idf.setReducerClass(IdfReducer.class);
		job_idf.setInputFormatClass(TextInputFormat.class);
		job_idf.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		job_idf.setJarByClass(TermFrequencyDriver.class);
		job_idf.setMapOutputKeyClass(Text.class);
		job_idf.setMapOutputValueClass(Text.class);
		job_idf.setOutputKeyClass(Text.class);
		job_idf.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job_idf, new Path(args[1].toString()+"/part-r-00000"));
		System.err.println("In driver read the file successfully");
		FileOutputFormat.setOutputPath(job_idf, new Path(args[2]));   //.setOutputPath(job_idf, new Path(args[2].toString()));
		job_idf_complete = job_idf.waitForCompletion(true);
				
		
		
		}
		
		return (job_idf_complete ?0: -1);
		
		
		
		
		
	}
	

}
