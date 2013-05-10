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


public class PreMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String[] center;
	
	protected void setup(Context context) throws IOException,InterruptedException  //read centerlist, and save to center[]
	{
		String centerlist = "hdfs://localhost:9000/FKmeans/center/center"; //center文件
    	Configuration conf1 = new Configuration(); 
    	conf1.set("hadoop.job.ugi", "hadoop-user,hadoop-user"); 
       FileSystem fs = FileSystem.get(URI.create(centerlist),conf1); 
       FSDataInputStream in = null; 
       ByteArrayOutputStream out = new ByteArrayOutputStream();
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
			String[] val=outValue.split("   ");
			String vals="";
			for(int i=0;i<val.length-1;i++)
			{
				val[i]=""+Double.parseDouble(val[i])*1;
				vals+=(val[i]+"   ");
			}
			val[val.length-1]=""+Double.parseDouble(val[val.length-1])*1;
			vals+=(val[val.length-1]);
			Random rd = new Random();
			int sum=1000000;
			String va="";
			for(int i=0;i<center.length-1;i++){
				int tmp=rd.nextInt(sum);
				sum-=tmp;
				va+=(tmp*1.0/1000000 + "   ");
			}
		    va+=""+sum*1.0/1000000;
			
			InputSplit inputsplit=context.getInputSplit();
			String fileName=((FileSplit) inputsplit).getPath().getName();
			context.write(new Text(vals+"&"+va+"_"+fileName),new Text(""));
	}

}
