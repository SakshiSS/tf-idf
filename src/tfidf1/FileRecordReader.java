package tfidf1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;


public class FileRecordReader extends RecordReader<FileInputWritable,Text>{
	
	
	private long start = 0;
	private long end = 0;
	private long pos=0;
	private FileInputWritable key;
	private Text value = new Text();
	private FileSystem fs ;
	private Path path;	
	private int maxLength ;
	boolean fProcessed ;
	//RemoteIterator<LocatedFileStatus> status = null;
	
	Configuration conf;
	CombineFileSplit split = null;
	private FSDataInputStream fdStream ;
	private LineReader lineReader;
	private org.apache.hadoop.mapreduce.lib.input.LineRecordReader lineRecordReader ;
	private long maxLineLength;
	FileSplit fileSplit;
	
	
	
	public FileRecordReader(CombineFileSplit split, TaskAttemptContext context,  Integer index) throws IOException {
		
		lineRecordReader = new org.apache.hadoop.mapreduce.lib.input.LineRecordReader();
		this.path = split.getPath(index);
	
		fs = this.path.getFileSystem(context.getConfiguration());
		this.start = split.getOffset(index);
		this.end = start + split.getLength(index);
		this.fdStream = fs.open(path);
		this.lineReader = new LineReader(fdStream);
		this.pos = start;
		fileSplit = new FileSplit(path, start, split.getLength(index), split.getLocations());
		lineRecordReader.initialize(fileSplit, context);
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lineRecordReader.close(); 
		
	}

	@Override
	public FileInputWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(start == end)
			return 0.0f;
		else
			return Math.min(1.0f, (pos - start) / (float)(end - start)); 
		
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		 split = (CombineFileSplit) inputSplit;
		 conf = context.getConfiguration();				
			
		
		
			
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (key == null) {
		  key = new FileInputWritable();
		    key.fileName =path.getName();
		  }
		  key.fileOffSet = pos;
		  if (value == null){
		    value = new Text();
		  }
		  int newSize = 0;
		  if (pos < end) {
		    newSize = lineReader.readLine(value);
		    pos += newSize;
		  }
		  if (newSize == 0) {
		    key = null;
		    value = null;
		    return false;
		  } else{
		    return true;
		  }

}
}
