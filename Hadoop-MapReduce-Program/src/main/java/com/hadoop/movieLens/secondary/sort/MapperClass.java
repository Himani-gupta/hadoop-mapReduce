package com.hadoop.movieLens.secondary.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, CustomKey, Text> {

	private CustomKey customkey = new CustomKey();
	private Text usersList = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\\t");
		customkey.setMovieId(Integer.parseInt(tokens[1]));
		customkey.setRating(Integer.parseInt(tokens[2]));
		usersList.set(tokens[0]);
		context.write(customkey, usersList);
	}

}
