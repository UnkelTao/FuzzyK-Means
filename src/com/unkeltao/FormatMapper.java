package com.unkeltao;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
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
import com.unkeltao.FKmeansTool;

public class FormatMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	private String[] center;
	private int K,VK;
	
	double C0[];
	protected void setup(Context context) throws IOException,InterruptedException  //read centerlist, and save to center[]
	{
		String centerlist = "/FKmeans/center/center"; //center文件
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
       K=center.length;
       VK = center[0].split(" ").length;
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			int ans=0;
			String val[] = outValue.split("&")[1].split(" ");
		    double  mx = Double.parseDouble(val[0]);
			for(int i=1;i<K;i++){
				double p=Double.parseDouble(val[i]);
				if(mx-p<0.0) {mx=p; ans=i;} 
			}
			context.write(key,new Text(""+(ans+1)));
	}

}
