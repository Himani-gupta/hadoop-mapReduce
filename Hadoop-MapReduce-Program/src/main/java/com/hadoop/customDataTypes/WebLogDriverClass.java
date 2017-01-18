package com.hadoop.customDataTypes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLogDriverClass {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg);
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(cfg);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}
		job.setJarByClass(WebLogDriverClass.class);

		job.setMapperClass(WebLogMapperClass.class);
		job.setReducerClass(WebLogReducerClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(WebLogWritable.class);
		//Not mandatory to set map output value class if not using custom data type
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(4);
		
		job.setPartitionerClass(WebLogPartitionerClass.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
