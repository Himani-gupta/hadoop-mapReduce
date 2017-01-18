package com.hadoop.customDataTypes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapperClass extends Mapper<LongWritable, Text, WebLogWritable, IntWritable> {

	private WebLogWritable wLog = new WebLogWritable();
	private IntWritable one = new IntWritable(1);

	private IntWritable reqno = new IntWritable();
	private Text url = new Text();
	private Text rdate = new Text();
	private Text rtime = new Text();
	private Text rip = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String words[] = value.toString().split("\t");
		url.set(words[0]);
		rdate.set(words[1]);
		rtime.set(words[2]);
		rip.set(words[3]);
		reqno.set(Integer.parseInt(words[4]));		
		wLog.set(url, rdate, rtime, rip, reqno);
		context.write(wLog, one);
	}

}