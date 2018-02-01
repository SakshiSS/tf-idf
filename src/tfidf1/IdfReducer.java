package tfidf1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IdfReducer extends Reducer<Text,Text,Text,Text>{
	int doc_cnt=0;
	Map<String,Integer> maintaindoccount=new HashMap<String,Integer>();
	Map<String,Double> tfList=new HashMap<String,Double>();
	Map<String,List<String>> maintaintextfiles=new HashMap<String,List<String>>();
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		
		int termdocument=0;
		List<String> textfiles=new ArrayList<String>();
		String tf="";
	if(!key.toString().equals("fc")){	
		for(Text v:values){
			//context.write(key,v);
		    String[] temp=v.toString().split("-");
		    String doc_id=temp[0];
		    tf=temp[1];
		    tfList.put(key.toString(),Double.parseDouble(tf));
	        textfiles.add(doc_id+"-"+tf);
	        termdocument++;
		}//for
		maintaintextfiles.put(key.toString(),textfiles);
		maintaindoccount.put(key.toString(),termdocument);
	}//if
	if(key.toString().equals("fc")){
		Iterator<Text> itr = values.iterator();
		itr.hasNext();
		String value=itr.next().toString();
		String[] tmp=value.split("-");
		doc_cnt=Integer.parseInt(tmp[1]);
	}//if
		//context.write(key, new LongWritable(nt));
		
	}//reduce
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		//Integer N=m.get("TotalCount");
		//m.remove("TotalCount");
		//System.out.println("N::"+N);
		/*for(Entry<String,Integer> e:.entrySet()){
			//System.out.println(e.getKey()+"-->"+e.getValue());
			double cal=N/(e.getValue());
			double idf=Math.log(cal)/Math.log(2.0d);
			List<String> s1=m1.get(e.getKey());
			for(int i=0;i<s1.size();i++){
				String op=s1.get(i)+" "+idf;
				context.write(new Text(e.getKey()), new Text(op));
				
			}//for
		}//for*/
		/*for(Entry<String,Integer> e:maintaindoccount.entrySet()){
			List<String> s1=maintaintextfiles.get(e.getKey());
			for(int i=0;i<s1.size();i++){
				
				context.write(new Text(e.getKey()), new Text(s1.get(i)+"-"+e.getValue()));
				
			}//for
		}//for*/
		for(Entry<String,Integer> e:maintaindoccount.entrySet()){
			List<String> s1=maintaintextfiles.get(e.getKey());
			Double tf=tfList.get(e.getKey());
			double cal=doc_cnt/(e.getValue());
			double idf=Math.log(cal)/Math.log(2.0d);
			double tfidf=tf*idf;
			for(int i=0;i<s1.size();i++){
				context.write(new Text(e.getKey()), new Text(s1.get(i)+"-"+tfidf));
				
			}//for
		}//for
		
	}//cleanup

}
