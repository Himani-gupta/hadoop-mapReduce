package com.hadoop.movieLens.mapSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerToFindRatedMovies extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

	Text ratingList = new Text();

	@Override
	public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}

}
