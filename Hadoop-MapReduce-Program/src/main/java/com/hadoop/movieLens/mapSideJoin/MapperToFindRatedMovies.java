package com.hadoop.movieLens.mapSideJoin;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperToFindRatedMovies extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

	private IntWritable movieId = new IntWritable();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\t");
		if (StringUtils.isNotEmpty(tokens[2])) {
			movieId.set(Integer.parseInt(tokens[1]));
			context.write(movieId, NullWritable.get());
		}
	}
}
