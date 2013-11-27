package com.unkeltao;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;





public class CenterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private double M;
	private String[] center;
	private int K,VK;
	private double X[][],Temp[];
	protected void setup(Context context) throws IOException,InterruptedException  
	{
		M=2;
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
       X=new double[K][VK]; 
       Temp=new double[K];
       for(int i=0;i<K;i++){
    	   Temp[i]=0;
    	   for(int j=0;j<VK;j++)
    		   X[i][j]=0;
       }
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			String[] arr =outValue.split("&"); 
			String val = arr[0];
			String[] U = arr[1].split("_")[0].split(" ");
			String[] x = val.split(" ");
			DecimalFormat df = new DecimalFormat("#0.00");
			for(int i=0;i<K;i++)
			{   
				double tmp=Math.pow(Double.parseDouble(U[i]),M);
				Temp[i]+=tmp;
				//context.write(new Text(i+"_"+x.length),new Text(val+"_"+df.format(tmp)));
				for(int j=0;j<VK;j++){
					X[i][j]+= Double.parseDouble(x[j])*tmp;
				}
			}
	}
	 protected void cleanup(Context context) throws IOException, InterruptedException { 
		 DecimalFormat df = new DecimalFormat("#0.00");
		 for(int i=0;i<K;i++){
			 String val="";
			 for(int j=0;j<VK-1;j++){
					val+=df.format(X[i][j])+" ";
				}
			 val+=df.format(X[i][VK-1])+"_"+df.format(Temp[i]);
			 context.write(new Text(i+"_"+VK), new Text(val));
		 }
	 }
}
