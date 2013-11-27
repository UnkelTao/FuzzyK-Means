package com.unkeltao;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FormatReducer extends Reducer<LongWritable, Text, Text, Text> {
	
	
	public void reduce(LongWritable key,Iterable<Text> value,Context context) throws IOException,InterruptedException
	{
//		String[] Temp=key.toString().split("_");
		for(Text val:value)
		{
			context.write(val,new Text(""));
		}
	}

}
