package com.aws.hadoop.fileConverter;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class DatToCsvConverter {
	private static final Logger logger = Logger.getLogger(DatToCsvConverter.class);
	public static void main(String args[])
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileSystem hdfs = inputPath.getFileSystem(conf);
		logger.info("***************#####@@@@@@in driver class***************#####@@@@@@");
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}
		job.setJarByClass(DatToCsvConverter.class);
		job.setMapperClass(MapperClassWithAwsKinesisConfig.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		logger.trace("***************#####@@@@@@Driver is executed successfully***************#####@@@@@@");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
