package com.hadoop.movieLens.mapSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithCal extends Reducer<LongWritable, IntWritable, LongWritable, Text> {

	Text ratingList = new Text();

	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0, count = 0, max = 0, min = 100000;
		for (IntWritable value : values) {
			if (value.get() > max) {
				max = value.get();
			}
			if (value.get() < min) {
				min = value.get();
			}
			sum = sum + value.get();
			count++;
		}
		String avg = String.valueOf(sum / count);
		ratingList.set("MaxRating :" +max +"\t"+"MinRating :"+min+"\t"+"AverageRating :"+avg);
		context.write(key, ratingList);
	}
}
