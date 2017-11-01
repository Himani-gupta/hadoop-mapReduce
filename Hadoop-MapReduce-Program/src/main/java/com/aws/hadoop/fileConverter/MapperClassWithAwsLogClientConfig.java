package com.aws.hadoop.fileConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutLogEventsRequest;

public class MapperClassWithAwsLogClientConfig extends Mapper<LongWritable, Text, NullWritable, Text> {

	public static Calendar calendar = Calendar.getInstance();
	public static PutLogEventsRequest putRequest = null;
	public static AWSLogsClient logClient = null;
	public static int count = 0;

	
	protected void setup(Context context) throws IOException, InterruptedException {
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
			logClient = new AWSLogsClient(credential);
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/home/impadmin/.aws/credentials), and is in valid format.", e);
		}
		
		logClient.setRegion(Region.getRegion(Regions.US_EAST_1));
		putRequest = new PutLogEventsRequest();
		putRequest.setLogGroupName("CloudTrail/DefaultLogGroup");
		logger.error("***************#####@@@@@@config complete***************#####@@@@@@");

	}

	private static final Logger logger = Logger.getLogger(MapperClassWithAwsLogClientConfig.class);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String token = null;
		count++;
		putRequest.setLogStreamName("EMR"+count);
		logger.error("***************#####@@@@@@logging is working in map reduce***************#####@@@@@@");

		// here's mainly what I changed
		DescribeLogStreamsRequest logStreamsRequest = new DescribeLogStreamsRequest()
				.withLogGroupName(putRequest.getLogGroupName());
		List<LogStream> logStreamList = logClient.describeLogStreams(logStreamsRequest).getLogStreams();
		if (logStreamList != null && logStreamList.size() > 0) {
			for (LogStream logStream : logStreamList) {
				
				token = logStream.getUploadSequenceToken();
			}
			if (token != null) {
				putRequest.setSequenceToken(token);
			}
		}
		ArrayList<InputLogEvent> logEvents = new ArrayList<InputLogEvent>();
		String values[] = value.toString().split(Pattern.quote("::"));
		Text newVal = new Text(StringUtils.join(values, ','));
		context.write(NullWritable.get(), newVal);
		InputLogEvent log = new InputLogEvent();
		log.setMessage("logging is working in map");
		log.setTimestamp(calendar.getTimeInMillis());
		logEvents.add(log);
		logger.error("***************#####@@@@@@logging is working in map reduce***************#####@@@@@@");
		/*final AmazonCloudWatch cw = AmazonCloudWatchClientBuilder.defaultClient();

		Dimension dimension = new Dimension().withName("UNIQUE_PAGES").withValue("URLS");

		MetricDatum datum = new MetricDatum().withMetricName("PAGES_VISITED").withUnit(StandardUnit.None).withValue(1.0)
				.withDimensions(dimension);

		PutMetricDataRequest request = new PutMetricDataRequest().withNamespace("SITE/TRAFFIC").withMetricData(datum);

		PutMetricDataResult response = cw.putMetricData(request);*/
		logger.info("***************#####@@@@@@map task is complete***************#####@@@@@@");

		putRequest.setLogEvents(logEvents);
		
		// make the request
		logClient.putLogEvents(putRequest);
	}
}
