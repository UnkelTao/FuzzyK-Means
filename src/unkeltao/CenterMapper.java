package com.unkeltao;
import java.io.IOException;
import java.text.DecimalFormat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;





public class CenterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private double M;
	protected void setup(Context context) throws IOException,InterruptedException  
	{
		M=2;
		System.err.println(M+"              试试");
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			String[] arr =outValue.split("&"); 
			String val = arr[0];
			String[] U = arr[1].split("_")[0].split(" ");
			
			DecimalFormat df = new DecimalFormat("#0.00");
			for(int i=0;i<U.length;i++)
			{   
				double tmp=Math.pow(Double.parseDouble(U[i]),M);
				context.write(new Text(i+"_"+arr[0].split(" ").length),new Text(val+"_"+df.format(tmp)));
			}
	}
}
