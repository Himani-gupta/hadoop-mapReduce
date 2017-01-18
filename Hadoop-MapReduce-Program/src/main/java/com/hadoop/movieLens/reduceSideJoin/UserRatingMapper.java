package com.hadoop.movieLens.reduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRatingMapper extends Mapper<LongWritable, Text, CustomKey, Text> {

	private CustomKey customkey = new CustomKey();
	private Text userId = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\\t");
		customkey.setMovieId(Integer.parseInt(tokens[1]));
		customkey.setDataType(1);
		userId.set(tokens[0]);
		context.write(customkey, userId);
	}
}
