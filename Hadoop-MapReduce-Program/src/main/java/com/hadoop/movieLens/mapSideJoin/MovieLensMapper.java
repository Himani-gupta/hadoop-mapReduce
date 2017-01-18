package com.hadoop.movieLens.mapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieLensMapper extends Mapper<LongWritable, Text, Text, Text> {

	//private Text rating = new Text();
	private Text usersRating = new Text();
	private Text movieName = new Text();
	private Map<Integer, String> movieMap = new HashMap<Integer, String>();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\t");
		movieName.set(movieMap.get(Integer.parseInt(tokens[1])));
		//rating.set(tokens[2]);
		usersRating.set(tokens[0]);
		context.write(movieName, usersRating);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// loading movie map in context
		loadMovieDataInMemory(context);
	}

	private void loadMovieDataInMemory(Mapper<LongWritable, Text, Text, Text>.Context context) {

		try {
			BufferedReader br = new BufferedReader(new FileReader("movie"));
			String line;
			while ((line = br.readLine()) != null) {
				String columns[] = line.toString().split("\\|");
				movieMap.put(Integer.parseInt(columns[0]), columns[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}



