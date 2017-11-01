package com.hadoop.usecase.addSeqNum;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SeqNumDriver {
	public static void main(String args[])
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		conf.setInt("mapreduce.input.lineinputformat.linespermap", 10);
		Job job1 = Job.getInstance(conf);
		Path inputPath = new Path(args[0]);
		Path tempOutputDir = new Path(args[1]);
		Path finalOutputDir = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(tempOutputDir)) {
			hdfs.delete(tempOutputDir, true);
		}
		job1.setJarByClass(SeqNumDriver.class);
		job1.setMapperClass(AddSeqNumMapper.class);
		job1.setReducerClass(AddSeqNumReducer.class);
		job1.setNumReduceTasks(3);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, tempOutputDir);
		job1.setInputFormatClass(NLineInputFormat.class);

		if (job1.waitForCompletion(true)) {
			Configuration conf1 = new Configuration();
			if (FileSystem.get(conf1).exists(finalOutputDir)) {
				hdfs.delete(finalOutputDir, true);
			}
			Job job2 = Job.getInstance(conf1);
			job2.setMapperClass(SortEmployeeDataMapper.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, tempOutputDir);
			FileOutputFormat.setOutputPath(job2, finalOutputDir);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		ChainReducer.setReducer(job1, AddSeqNumReducer.class, LongWritable.class, Text.class, LongWritable.class,
				Text.class, new Configuration(false));
		ChainMapper.addMapper(job1, SortEmployeeDataMapper.class, LongWritable.class, Text.class, LongWritable.class,
				Text.class, null);
		ChainReducer.setReducer(job1, AddSeqNumReducer.class, LongWritable.class, Text.class, LongWritable.class,
				Text.class, new Configuration(false));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);

	}
}
