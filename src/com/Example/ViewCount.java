package com.Example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ViewCount extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration configuration=this.getConf();
		
		Job job=Job.getInstance(configuration);
		job.setJobName("viewCount");
		job.setJarByClass(ViewCount.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		Path inputFilePath=new Path(arg0[0]);
		Path outputFilePath=new Path(arg0[1]);
		
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
		
	}
	
	public static void main(String[] args) throws Exception
	{
		int exitcode=ToolRunner.run(new ViewCount(), args);
		System.exit(exitcode);
		
	}

}
