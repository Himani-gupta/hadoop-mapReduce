package com.hadoop.movieLens.secondary.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerClass extends Reducer<CustomKey, Text, IntWritable, Text> {

	@Override
	public void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		IntWritable movieId = new IntWritable(key.getMovieId());
		Text users = new Text();
		Set<String> usersList = new HashSet<String>();
		while (itr.hasNext()) {
			usersList.add(key.getRating() +"--"+itr.next().toString());			
		}
		users.set(StringUtils.join(usersList, ','));
		context.write(movieId , users);
	}

}
