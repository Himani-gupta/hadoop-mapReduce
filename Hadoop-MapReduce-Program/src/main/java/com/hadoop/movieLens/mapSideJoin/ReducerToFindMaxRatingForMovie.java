package com.hadoop.movieLens.mapSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerToFindMaxRatingForMovie extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

	IntWritable maxRating = new IntWritable();

	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int max = 0;
		for (IntWritable value : values) {
			if (value.get() > max) {
				max = value.get();
			}
		}
		maxRating.set(max);
		context.write(key, maxRating);
	}
}
