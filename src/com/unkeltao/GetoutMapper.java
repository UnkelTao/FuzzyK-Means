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


public class GetoutMapper extends Mapper<LongWritable, Text, Text, Text> {
	

	
	protected void setup(Context context) throws IOException,InterruptedException  //read centerlist, and save to center[]
	{
		
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
			String outValue = new String(value.toString());
			
			String[] vals = outValue.split("&");
			String val = vals[0]+"&"+vals[1].split("#")[1];
			context.write(new Text(val),new Text(""));
	}

}
