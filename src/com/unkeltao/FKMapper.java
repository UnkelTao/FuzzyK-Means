package com.unkeltao;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.Random;


public class FKMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String[] center;
	private double M;
	
	protected void setup(Context context) throws IOException,InterruptedException  //read centerlist, and save to center[]
	{
		String centerlist = "hdfs://localhost:9000/FKmeans/center/center"; //center文件
    	Configuration conf1 = new Configuration(); 
    	conf1.set("hadoop.job.ugi", "hadoop-user,hadoop-user"); 
       FileSystem fs = FileSystem.get(URI.create(centerlist),conf1); 
       FSDataInputStream in = null; 
       ByteArrayOutputStream out = new ByteArrayOutputStream();
       M=2;
       try{ 
             
           in = fs.open( new Path(centerlist) ); 
           IOUtils.copyBytes(in,out,100,false);  
           center = out.toString().split("\n");
           }finally{ 
                IOUtils.closeStream(in);
            }
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			String[] str=outValue.split("&");
		    String[] str2=str[1].split("_");
			String[] values = outValue.split("&")[0].split("   ");
	
			String va="";
			double sum=0;
			double[] Dis=new double[center.length];
			for(int i=0;i<center.length;i++){
				String[] c = center[i].split("   ");
				double temp=0;
				for(int j=0;j<c.length;j++)
				{
					temp+=Math.sqrt(Math.pow(Double.parseDouble(values[j])-Double.parseDouble(c[j]), 2));
				}
				Dis[i]=temp;
			}
			double[] U = new double[center.length];
			boolean flag=true;
			for(int i=0;i<center.length;i++)
			{
				U[i]=0;
				if(Dis[i]==0){flag=false;U[i]=1;}
			}
			if(flag)
			for(int i=0;i<center.length;i++)
			{   
				
				double temp=0;
				for(int j=0;j<center.length;j++){
					temp+=Math.pow(Dis[i]/Dis[j] ,2*1.0/(M-1));
				}
				U[i]=1/temp;
			}
			for(int i=0;i<U.length-1;i++)va+=(U[i]+"   ");
			va+=(U[U.length-1]+"");
			String keys = str[0].toString()+"&"+str2[0].toString()+"#"+va+"_"+str2[1].toString();
			context.write(new Text(keys),new Text(""));
	}
}
