package com.hadoop.customDataTypes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebLogReducerClass extends Reducer<WebLogWritable, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	private Text ip = new Text();

	@Override
	public void reduce(WebLogWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		ip = key.getIp();
		int sum = 0;
		for (IntWritable value : values) {
			sum++;
		}
		result.set(sum);
		context.write(ip, result);
	}

}
