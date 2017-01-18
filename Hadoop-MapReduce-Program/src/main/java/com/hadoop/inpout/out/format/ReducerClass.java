package com.hadoop.inpout.out.format;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			context.write(key, new Text("value"+value));
		}
	}
}
