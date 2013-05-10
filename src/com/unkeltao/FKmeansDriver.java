package com.unkeltao;

import static com.unkeltao.FKmeansTool.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.org.apache.bcel.internal.classfile.Code;


public class FKmeansDriver {
	public static void main(String[] args) throws Exception{
		
		/**
		 * @author unkeltao
		 * @job1 随即生成隶属度矩阵
		 */
		
		Configuration preProcessConf = new Configuration();
		preProcessConf.set(input_path_key, input_path);
		preProcessConf.set(output_path_key, output_path);
		
		if(!preprocess(preProcessConf)){
			System.err.println("error!");
			System.exit(1);
		}
		
		
		double s=1;
		int i=0;
		while(i++<time&&s>accuracy){
			
			//生成新的聚类中心
			Configuration centerprocessConf = new Configuration();
			centerprocessConf.set(input_path_key, input_path);
			centerprocessConf.set(output_path_key, output_path);
			if (!centerprocess(centerprocessConf)) {
				System.exit(1);
			}
			
			//将新的聚类中心放到center目录
			newcenter();
			
			//生成新的隶属度矩阵
			Configuration fkProcessConf = new Configuration();
			fkProcessConf.set(input_path_key, input_path);
			fkProcessConf.set(output_path_key, output_path);
			if(!fkprocess(fkProcessConf)){
				System.err.println("error!");
				System.exit(1);
			}
			
			//寻找最大的距离值
			Configuration cntUProcessconf = new Configuration();
			cntUProcessconf.set(input_path_key, input_path);
			cntUProcessconf.set(output_path_key, output_path);
			if(!cntuprocess(cntUProcessconf)){
				System.err.println("error!");
				System.exit(1);
			}
			
			s=getMax(); //读入最大值
			
			//重定义output文件
			Configuration getoutProcessconf = new Configuration();
			getoutProcessconf.set(input_path_key, input_path);
			getoutProcessconf.set(output_path_key, output_path);
			if(!getoutprocess(getoutProcessconf)){
				System.err.println("error!");
				System.exit(1);
			}
		}
		
		//聚类结果
		System.out.println("迭代次数:"+i);
		System.out.println("迭代精度:"+s);
	}
	









	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 随机生成隶属度矩阵的job设置
	 */
	static boolean preprocess(Configuration conf)throws Exception
	{
		Job job = new Job(conf,"Preprocess");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(PreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
//		job.setReducerClass(PreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(PreOutput.class);
		job.setNumReduceTasks(0);
		
		//删除已经存在的output
		Configuration confs = new Configuration();
		confs.set("fs.default.name", "hdfs://localhost:9000/");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/output"),true);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(input_path_key)+"input"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(output_path_key)+"output"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}
	
	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 生成聚类中心
	 */
	static boolean centerprocess(Configuration conf)throws Exception{

		Job job = new Job(conf,"Centerprocess");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CenterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CenterReducer.class);
		
		//删除已经存在的centeroutput
		Configuration confs = new Configuration();
		confs.set("fs.default.name", "hdfs://localhost:9000/");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/centeroutput"),true);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(input_path_key)+"output"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(input_path_key)+"centeroutput"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}
	
	/**
	 * @author unkeltao
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @function 替换新的聚类中心
	 */
	static boolean newcenter()throws IOException,InterruptedException{		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(URI.create("/FKmeans/centeroutput/part-r-00000"),conf);
		FSDataInputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{ 
	         
			in = fs.open( new Path("/FKmeans/centeroutput/part-r-00000")); 
			IOUtils.copyBytes(in,out,50,false);
		} finally { 
			IOUtils.closeStream(in);
		}
		System.out.println(out.toString());
		OutputStream out2 = fs.create(new Path("hdfs://localhost:9000/FKmeans/center/center") ); 
		IOUtils.copyBytes(new ByteArrayInputStream(out.toString().getBytes()), out2, 4096,true);
		return true;
	   }
	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 生成新的隶属度矩阵
	 */
	static boolean fkprocess(Configuration conf)throws Exception{
		Job job = new Job(conf,"fkprocess");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
//		job.setReducerClass(FKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(PreOutput.class);
		job.setNumReduceTasks(0);
		
		//删除已经存在的output2
		Configuration confs = new Configuration();
		confs.set("fs.default.name", "hdfs://localhost:9000/");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/output2"),true);
		FileInputFormat.addInputPath(job, new Path(conf.get(input_path_key)+"output"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(output_path_key)+"output2"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}
	
	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 计算最大的隶属度差值 U(L+1)-U(L)
	 */
	private static boolean cntuprocess(Configuration conf)throws Exception {
		
		Job job = new Job(conf,"Cntuprocess");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CntMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CntReducer.class);
		
		//删除已经存在的maxsuboutput
		Configuration confs = new Configuration();
		confs.set("fs.default.name", "hdfs://localhost:9000/");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/maxsuboutput"),true);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(input_path_key)+"output2"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(input_path_key)+"maxsuboutput"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}
	
	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 更新隶属度矩阵
	 */
	private static boolean getoutprocess(Configuration conf) throws Exception{
		Job job = new Job(conf,"getoutprocess");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(GetoutMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
//		job.setReducerClass(GetoutReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(PreOutput.class);
		job.setNumReduceTasks(0);
		
		//删除已经存在的output2
		Configuration confs = new Configuration();
		confs.set("fs.default.name", "hdfs://localhost:9000/");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/output"),true);
		
		System.err.println("删除成功");
		FileInputFormat.addInputPath(job, new Path(conf.get(input_path_key)+"output2"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(output_path_key)+"output"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}

	/**
	 * @author unkeltao
	 * @return
	 * @throws Exception
	 * @function 从文件中读取最大值
	 */
	private static double getMax() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(URI.create("/FKmeans/maxsuboutput/part-r-00000"),conf);
		FSDataInputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		double result=0;
		String line;
		try{ 
	         
			in = fs.open(new Path("/FKmeans/maxsuboutput/part-r-00000")); 
			IOUtils.copyBytes(in,out,50,false);
			line = out.toString().split("\n")[0];
		} finally { 
			IOUtils.closeStream(in);
		}
		System.out.println("line:"+line);
		result = Double.parseDouble(line);
		return result;
	}

}
