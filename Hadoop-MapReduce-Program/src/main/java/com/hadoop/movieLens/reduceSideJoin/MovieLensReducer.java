package com.hadoop.movieLens.reduceSideJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensReducer extends Reducer<CustomKey, Text, Text, Text> {

	@Override
	public void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		Text movieName = new Text(itr.next());
		Text users = new Text();
		List<String> usersList = new ArrayList<String>();
		while (itr.hasNext()) {
			usersList.add(itr.next().toString());			
		}
		users.set(StringUtils.join(usersList, ','));
		context.write(movieName , users);
	}

}
