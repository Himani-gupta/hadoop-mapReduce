package com.hadoop.movieLens.mapSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperToFindMaxRatingForMovie extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

	private IntWritable movieRating = new IntWritable();
	private LongWritable movieId = new LongWritable();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\t");
		movieId.set(Long.parseLong(tokens[1]));
		movieRating.set(Integer.parseInt(tokens[2]));
		context.write(movieId, movieRating);
	}
}
