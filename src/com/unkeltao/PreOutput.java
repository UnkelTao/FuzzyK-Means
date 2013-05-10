package com.unkeltao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class PreOutput extends MultipleOutputFormat<Text, Text> {  
	@Override
		protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) { 
			String[] name=key.toString().split("_");
			return name[1];
		}
		
	}