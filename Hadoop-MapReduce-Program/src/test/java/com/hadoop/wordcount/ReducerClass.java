package com.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable totalWordCount = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int wordCount = 0;
		for (IntWritable value : values) {
			wordCount += value.get();
			totalWordCount.set(wordCount);		
		}
		context.write((Text) key, totalWordCount);

	}
}
