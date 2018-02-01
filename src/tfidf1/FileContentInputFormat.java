package tfidf1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


@SuppressWarnings("deprecation")
public class FileContentInputFormat extends CombineFileInputFormat<FileInputWritable, Text> {
	
	public FileContentInputFormat(){
		super();
	}
	
	protected boolean isSplitable(JobContext context, Path file) {
		return false; 
	}
	
	
	
	public RecordReader<FileInputWritable, Text> createRecordReader(InputSplit split,TaskAttemptContext context ) throws IOException{
		CombineFileRecordReader<FileInputWritable, Text> fr = new CombineFileRecordReader<FileInputWritable,Text>((CombineFileSplit)split, context, FileRecordReader.class); 
				//new FileRecordReader((CombineFileSplit) split,context, null); //remove null
		//fr.initialize(split, context);
		return fr;
		
	}



	

	

	

}
