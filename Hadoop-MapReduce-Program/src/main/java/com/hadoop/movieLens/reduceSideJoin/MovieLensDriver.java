package com.hadoop.movieLens.reduceSideJoin;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieLensDriver {

	public static void main(String args[])
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		Path inputPath = new Path(args[0]);
		Path inputPath1 = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}
		job.setJarByClass(MovieLensDriver.class);
		job.setReducerClass(MovieLensReducer.class);
		MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, UserRatingMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
