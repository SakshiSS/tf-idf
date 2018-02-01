package tfidf1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;


public class CustomRecordFormat extends RecordReader<Text,Text> {
	
	private FileSplit fileSplit;
	private Configuration conf;
	private StringBuffer fileContent = new StringBuffer();
	
	private long startPosition ;
	private long position;
	private long endPosition;
	
	
	private LineReader lineReader;
	private int maxLineLength;
	private Text key = new Text();
	private Text value = new Text();
	private RecordReader<Text, Text> recordReader;
	
	
	

	@Override
	public void close() throws IOException {
		this.recordReader.close();
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.recordReader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		 fileSplit = (FileSplit) arg0;
		 conf = arg1.getConfiguration();			
		
		this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
		this.startPosition = fileSplit.getStart();
		this.endPosition = startPosition + fileSplit.getLength();	
		
		
		this.recordReader.initialize(arg0, arg1);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		fileContent = null;
		byte[] contents = new byte[(int) fileSplit.getLength()];
		Path filePath = fileSplit.getPath();
		FileSystem fs = filePath.getFileSystem(conf);
	
		FSDataInputStream fdInputStream = null;
		fdInputStream = fs.open(filePath);
		lineReader = new LineReader(fdInputStream,conf);
		//IOUtils.readFully(fdInputStream, contents, 0, contents.length);
		this.value.set(contents, 0, contents.length);
		
		return true;
		
		
	}
	
	

}
