package com.hadoop.usecase.addSeqNum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortEmployeeDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable seqNum = new LongWritable();
	private Text empData = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String token[] = value.toString().split("\\t");
		//String token[] = value.toString().split(",");
		System.out.println(value.toString());
		seqNum.set(Long.parseLong(token[0]));
		empData.set(token[1]+" "+token[2]);
		//empData.set(token[1]);
		context.write(seqNum, empData);
	}
}
