package com.unkeltao;

import static com.unkeltao.FKmeansTool.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
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
		Date d1=new Date();
		System.out.println(d1.getTime());
		
		
		Configuration preProcessConf = new Configuration();
		preProcessConf.set(input_path_key, input_path);
		preProcessConf.set(output_path_key, output_path);
		
		if(!preprocess(preProcessConf)){
			System.err.println("error!");
			System.exit(1);
		}
		
		System.out.println("/*********************************************************************************/");
		System.out.println("运行时间:"+(new Date().getTime()-d1.getTime()));
		System.out.println("运行时间:"+(new Date().getTime()-d1.getTime()));
		System.out.println("/*********************************************************************************/");
		
		
		double s=1;
		int i=0;
		String center[];
		time = Integer.parseInt(args[0]);
		while(i<time&&s>accuracy){
			i++;
			//生成新的聚类中心
			Configuration centerprocessConf = new Configuration();
			centerprocessConf.set(input_path_key, input_path);
			centerprocessConf.set(output_path_key, output_path);
			if (!centerprocess(centerprocessConf)) {
				System.exit(1);
			}
			
			center = getcenter();
			System.out.println("原始中心：");
			for(int pp=0;pp<center.length;pp++)
			   System.out.println(center[pp]);
			//将新的聚类中心放到center目录，并且返回最大差值
			s= newcenter(center);
			
			//生成新的隶属度矩阵
			Configuration fkProcessConf = new Configuration();
			fkProcessConf.set(input_path_key, input_path);
			fkProcessConf.set(output_path_key, output_path);
			if(!fkprocess(fkProcessConf)){
				System.err.println("error!");
				System.exit(1);
			}
		}
		
		Configuration fotmatProcessConf = new Configuration();
		preProcessConf.set(input_path_key, input_path);
		preProcessConf.set(output_path_key, output_path);
		
		if(!formatprocess(fotmatProcessConf)){
			System.err.println("error!");
			System.exit(1);
		}
		
		System.out.println(new Date().getTime());
		//聚类结果
		System.out.println("迭代次数:"+i);
		System.out.println("迭代精度:"+s);
		System.out.println("运行时间:"+(new Date().getTime()-d1.getTime())); 
		
		
	}
	


    /**
     * @author unkeltao
     * @return 聚类中心
     * @throws IOException
     * @throws InterruptedException
     */
    static String [] getcenter() throws IOException,InterruptedException {
    	String center[];
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
       return center;
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
		Job job = new Job(conf,"数据预处理Job");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(PreMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PreReducer.class);
	
		
		//删除已经存在的centeroutput
		Configuration confs = new Configuration();
	//	confs.set("fs.default.name", "");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/output"),true);
		

		FileInputFormat.addInputPath(job, new Path(input_path+"input"));
		FileOutputFormat.setOutputPath(job, new Path(input_path+"output"));
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

		Job job = new Job(conf,"生成聚类中心Job");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CenterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CenterReducer.class);
		
		//删除已经存在的centeroutput
		Configuration confs = new Configuration();
	//	confs.set("fs.default.name", "");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/centeroutput"),true);
		
		FileInputFormat.addInputPath(job, new Path(input_path+"output"));
		FileOutputFormat.setOutputPath(job, new Path(input_path+"centeroutput"));
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
	static double newcenter(String center2[])throws IOException,InterruptedException{		
		Configuration conf = new Configuration();
	//	conf.set("fs.default.name", "");
		String center1[];
		
		FileSystem fs = FileSystem.get(URI.create("/FKmeans/centeroutput/part-r-00000"),conf);
		FSDataInputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{ 
	         
			in = fs.open( new Path("/FKmeans/centeroutput/part-r-00000")); 
			IOUtils.copyBytes(in,out,50,false);
			  center1 = out.toString().split("\n");
		} finally { 
			IOUtils.closeStream(in);
		}
		
		
		
		System.out.println(out.toString());
		OutputStream out2 = fs.create(new Path("/FKmeans/center/center") ); 
		IOUtils.copyBytes(new ByteArrayInputStream(out.toString().getBytes()), out2, 4096,true);
		
		double Dis=0;
		for(int i=0;i<center1.length;i++){
			double tp=dis(center1[i],center2[i]);
			if(Dis<tp) Dis = tp; 
		}
		
		System.out.println("距离最大差值为： "+ Dis);
		return Dis;
	   }

	
	 static double dis(String s1,String s2){
		 String  p1[] = s1.split(" ");
		 String  p2[] = s2.split(" ");
		 double dis=0;
		 for(int i=0;i<p1.length;i++){
			 double l1=Double.parseDouble(p1[i]);
			 double l2=Double.parseDouble(p2[i]);
			 dis+=(l1-l2)*(l1-l2);
		 }
		 return Math.sqrt(dis);
	 }
	 
	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 生成新的隶属度矩阵
	 */
	static boolean fkprocess(Configuration conf)throws Exception{
		Job job = new Job(conf,"更新权重矩阵Job");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FKMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FKReducer.class);
		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(PreOutput.class);
//		job.setNumReduceTasks(0);
		
		//删除已经存在的output2
		Configuration confs = new Configuration();
	//	confs.set("fs.default.name", "");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/output"),true);
		
		FileInputFormat.addInputPath(job, new Path(input_path+"input"));
		FileOutputFormat.setOutputPath(job, new Path(input_path+"output"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}
	
	/**
	 * @author unkeltao
	 * @param conf
	 * @return
	 * @throws Exception
	 * @function 随机生成隶属度矩阵的job设置
	 */
	static boolean formatprocess(Configuration conf)throws Exception
	{
		Job job = new Job(conf,"最后格式化Job");
		job.setJarByClass(FKmeansDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FormatMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FormatReducer.class);
	
		
		//删除已经存在的centeroutput
		Configuration confs = new Configuration();
	//	confs.set("fs.default.name", "");
		FileSystem fs = FileSystem.get(confs);
		fs.delete(new Path("/FKmeans/format"),true);
		

		FileInputFormat.addInputPath(job, new Path(input_path+"output"));
		FileOutputFormat.setOutputPath(job, new Path(input_path+"format"));
		job.waitForCompletion(true);
		return job.waitForCompletion(true);
	}
	

	

}
