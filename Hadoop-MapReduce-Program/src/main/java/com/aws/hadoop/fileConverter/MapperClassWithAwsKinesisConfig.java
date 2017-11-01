package com.aws.hadoop.fileConverter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.logs.AWSLogsClient;

public class MapperClassWithAwsKinesisConfig extends Mapper<LongWritable, Text, NullWritable, Text> {

	public static Calendar calendar = Calendar.getInstance();
	private static AmazonKinesisClient kinesisClient;
	public final String myStreamName = "SampleStream";
	public final Integer shardCount = 1;
	public int count = 0;

	protected void setup(Context context) throws IOException, InterruptedException {
		AWSCredentials credentials = null;
		try {
			AWSCredentials credential = new AWSCredentials() {

				@Override
				public String getAWSSecretKey() {
					// TODO Auto-generated method stub
					return "YwXS8jI+mhiPNXc8iOpgiE36c5n9+Ki9vOf+hyeX";
				}

				@Override
				public String getAWSAccessKeyId() {
					// TODO Auto-generated method stub
					return "AKIAJ2FSFVOOYREGODQA";
				}
			};
			kinesisClient = new AmazonKinesisClient(credential);
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " + e);
		}
	}

	private static final Logger logger = Logger.getLogger(MapperClassWithAwsLogClientConfig.class);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(myStreamName);
		putRecordRequest
				.setData(ByteBuffer.wrap((calendar.getTimeInMillis() + "----" + "Mapper execution start").getBytes()));
		putRecordRequest.setPartitionKey(String.format("partitionKey-%d", count++));
		kinesisClient.putRecord(putRecordRequest);
		logger.error("***************#####@@@@@@logging is working in map reduce***************#####@@@@@@");
		String values[] = value.toString().split(Pattern.quote("::"));
		Text newVal = new Text(StringUtils.join(values, ','));
		context.write(NullWritable.get(), newVal);
		putRecordRequest.setData(
				ByteBuffer.wrap((calendar.getTimeInMillis() + "----" + "Mapper execution complete").getBytes()));
		kinesisClient.putRecord(putRecordRequest);
		logger.info("***************#####@@@@@@map task is complete***************#####@@@@@@");
	}
}
