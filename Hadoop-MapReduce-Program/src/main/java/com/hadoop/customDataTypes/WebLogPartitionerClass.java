package com.hadoop.customDataTypes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WebLogPartitionerClass extends Partitioner<WebLogWritable, IntWritable> {

	@Override
	public int getPartition(WebLogWritable key, IntWritable value, int numPartitions) {
		return (key.getIp().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
