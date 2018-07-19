package com.Example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException { 
		
		int count=0;
		for(IntWritable value:arg1)
		{
			count+=value.get();	
		}
		arg2.write(arg0, new IntWritable(count));
		
	}


}
