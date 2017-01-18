package com.hadoop.movieLens.mapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFor5thquery extends Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable movieId = new LongWritable();
	private Text userId = new Text();

	private Map<Integer, String> movieMaxRatingMap = new HashMap<Integer, String>();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split("\t");
		if (tokens[2].equals(movieMaxRatingMap.get(Integer.parseInt(tokens[1])))) {
			movieId.set(Long.parseLong(tokens[1]));
			userId.set(tokens[0]);
			context.write(movieId, userId);
		}
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// loading movie map in context
		loadMovieDataInMemory(context);
	}

	private void loadMovieDataInMemory(Mapper<LongWritable, Text, LongWritable, Text>.Context context) {

		try {
			BufferedReader br = new BufferedReader(new FileReader("moviemaxrating"));
			String line;
			while ((line = br.readLine()) != null) {
				String columns[] = line.toString().split("\\t");
				movieMaxRatingMap.put(Integer.parseInt(columns[0]), columns[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
