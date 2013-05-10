package com.unkeltao;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PreReducer extends Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
	{
//		String[] Temp=key.toString().split("_");
		for(Text val:value)
		{
			context.write(key,val);
		}
	}

}
