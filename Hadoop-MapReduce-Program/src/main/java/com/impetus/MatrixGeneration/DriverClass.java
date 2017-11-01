package com.impetus.MatrixGeneration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DriverClass {
	
	public static void main(String args[])
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg);
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileSystem hdfs = FileSystem.get(cfg);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}
		job.setJarByClass(DriverClass.class);
		job.setMapperClass(InputMatrixMapper.class);
		job.setReducerClass(InputMatrixReducer.class);
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(CustomerProductIndexPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(CustomerGroupComparator.class);
		
		job.addCacheFile(new URI("hdfs://localhost:54310/crosssell_upsell/cust-product-data/productIndexes"));

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
