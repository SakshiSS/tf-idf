package tfidf1;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FilesTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//Path path = new Path(args[0]);
		
		Path path = new Path(args[0]);
		org.apache.hadoop.fs.FileSystem fs =  path.getFileSystem(conf);
		InputStream inputStream = null;

		inputStream = fs.open(path);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		
		
		System.out.println("The file name is " + path.toString());
		System.out.println("The contents are ");
		IOUtils.copyBytes(inputStream, System.out, 4096, false);		
		
		

		
	}

}
