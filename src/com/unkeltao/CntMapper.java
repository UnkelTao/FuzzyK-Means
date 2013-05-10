package com.unkeltao;
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;





public class CntMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private double M;
	protected void setup(Context context) throws IOException,InterruptedException  
	{
		M=2;
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			String[] arr =outValue.split("&"); 
			String val = arr[1].split("_")[0].toString();
			String[] U1 = val.split("#")[0].split("   ");
			String[] U2 = val.split("#")[1].split("   ");
			
			double max=-1;
			for(int i=0;i<U1.length;i++)
			{   
				double tmp=Math.abs(Double.parseDouble(U1[i])-Double.parseDouble(U2[i]));
				max = max<tmp?tmp:max;
			}
			
			context.write(new Text("1"),new Text(max+""));
	}
}
