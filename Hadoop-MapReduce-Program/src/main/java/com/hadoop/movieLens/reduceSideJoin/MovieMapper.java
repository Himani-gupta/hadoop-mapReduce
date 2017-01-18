package com.hadoop.movieLens.reduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, CustomKey, Text>{
	
	private CustomKey customkey = new CustomKey();
	private Text movieName = new Text();
	@Override
	  public void map(LongWritable key, Text value, 
	                     Context context) throws IOException, InterruptedException {
		 String tokens[] = value.toString().split("\\|");
		 customkey.setMovieId(Integer.parseInt(tokens[0]));
		 customkey.setDataType(0);
		 movieName.set(tokens[1]);
	     context.write(customkey ,movieName);
	  }
}
