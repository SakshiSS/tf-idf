package tfidf1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class FullFileCustomInputFormat  extends FileInputFormat<Text,Text> {
	
	
	
	protected boolean isSplitable(JobContext context, Path file) {
		return false; 
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		return (RecordReader<Text, Text>) new CustomRecordFormat();
	}

}
