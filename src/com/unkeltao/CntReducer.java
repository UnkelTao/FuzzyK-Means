package com.unkeltao;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CntReducer extends Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException//�����ĳ������������ý�����е㡣����Щ���еĵ���ƽ��ֵ������µ�����.
	{
		double  max=-1;
		for(Text val:value)
		{   
			System.err.println("val: "+val);
			max = Math.max(max, Double.parseDouble(val.toString()));
		}
		context.write(new Text(max+""),new Text(""));
	}

}
