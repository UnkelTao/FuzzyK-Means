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

public class PreMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
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
     /*  C0 = new double[VK];
       String tp[] = center[0].split(" ");
       for(int i=0;i<VK;i++){
          C0[i] = Double.parseDouble(tp[i]);
       }
       */
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			if(outValue.split(" ").length == VK){
		   /*	
			String[] val=outValue.split(" ");
			String vals="";
			for(int i=0;i<VK-1;i++)
			{
				val[i]=""+Double.parseDouble(val[i])*1/C0[i];
				vals+=(val[i]+" ");
			}
			val[VK-1]=""+Double.parseDouble(val[val.length-1])*1/C0[VK-1];
			vals+=(val[val.length-1]); 
			*/
			
			Random rd = new Random();
			int sum=1000;
			String va="";
			DecimalFormat df = new DecimalFormat("#0.000");
			for(int i=0;i<center.length-1;i++){
				int tmp=rd.nextInt(sum);
				sum-=tmp;
				va+=(df.format(tmp*1.0/1000) + " ");
			}
			
		    va+=""+df.format(sum*1.0/1000);
			
		   
			context.write(key,new Text(outValue+"&"+va));
			}
	}

}
