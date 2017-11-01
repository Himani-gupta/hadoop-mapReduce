package com.hadoop.usecase.addSeqNum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddSeqNumMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private Text empDataWithOffset = new Text();
	private int recordNo = 0;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		empDataWithOffset.set(key + " " + value);
		context.write(new LongWritable(++recordNo), empDataWithOffset);
	}

}
