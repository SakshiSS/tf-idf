package tfidf1;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FileInputWritable implements WritableComparable<FileInputWritable>{

	public long fileOffSet;
	public String fileName;
	@Override
	public void readFields(DataInput input) throws IOException {
		this.fileOffSet = input.readLong();
		this.fileName = Text.readString(input);
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeLong(fileOffSet);
		Text.writeString(output, fileName);
		
	}

	@Override
	public int compareTo(FileInputWritable alreadyVisited) {
		// TODO Auto-generated method stub
		int compare = this.fileName.compareTo(alreadyVisited.fileName);
		if(compare != 0)
			return compare;
		return (int)Math.signum((double)(this.fileOffSet - alreadyVisited.fileOffSet));
		
	}
	
	
	
	

}
