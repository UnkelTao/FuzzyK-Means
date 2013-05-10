package com.unkeltao;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CenterReducer extends Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException//�����ĳ������������ý�����е㡣����Щ���еĵ���ƽ��ֵ������µ�����.
	{
		double[] Xs=new double[Integer.parseInt(key.toString().split("_")[1])];
		double  sum=0;
		for(int i=0;i<Xs.length;i++)Xs[i]=0;
		for(Text val:value)
		{
			String[] tmp = val.toString().split("_");
			sum+=Double.parseDouble(tmp[1]);
			String[] X=tmp[0].split("   ");
			for(int i=0;i<X.length;i++)
			{
				Xs[i]+=Double.parseDouble(X[i])*Double.parseDouble(tmp[1]);
			}
		}
		String center="";
		for(int i=0;i<Xs.length-1;i++)
		{
			Xs[i]=Xs[i]/sum;
			center+=(Xs[i]+"   ");
		}
		center+=(Xs[Xs.length-1]/sum+"");
		
		//System.out.println(center);
		
		context.write(new Text(center),new Text(""));
	}

}
