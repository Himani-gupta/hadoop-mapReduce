package com.hadoop.inpout.out.format;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("fdddddddddddddddddddd" + context.getInputSplit());
		System.out.println(context.getInputSplit().getLength());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
