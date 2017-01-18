package com.hadoop.movieLens.mapSideJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerFor5thQuery extends Reducer<LongWritable, Text, LongWritable, Text> {

	Text ratingList = new Text();

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<String> rating = new ArrayList<String>();
		for (Text value : values) {
			rating.add(value.toString());
		}
		ratingList.set(StringUtils.join(rating, ','));
		context.write(key, ratingList);
	}

}
