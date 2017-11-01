package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamDescription;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

public class AmazonKinesisFirehoseRecordProducerSample {

	 private static AmazonKinesisFirehoseClient kinesis;

	    private static void init() throws Exception {
	        AWSCredentials credentials = null;
			try {
				credentials = new ProfileCredentialsProvider("default").getCredentials();
			} catch (Exception e) {
				throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " + e);
			}

	        kinesis = new AmazonKinesisFirehoseClient(credentials);
	    }
	    public static void main(String[] args) throws Exception {
	        init();

	        final String myStreamName = "Stream-Data";

	        // Describe the stream and check if it exists.
	        DescribeDeliveryStreamRequest describeStreamRequest = new DescribeDeliveryStreamRequest().withDeliveryStreamName(myStreamName);
	        try {
	            DeliveryStreamDescription streamDescription = kinesis.describeDeliveryStream(describeStreamRequest).getDeliveryStreamDescription();
	            System.out.printf("Stream %s has a status of %s.\n", myStreamName, streamDescription.getDeliveryStreamStatus());

	            if ("DELETING".equals(streamDescription.getDeliveryStreamStatus())) {
	                System.out.println("Stream is being deleted. This sample will now exit.");
	                System.exit(0);
	            }

	            // Wait for the stream to become active if it is not yet ACTIVE.
	            if (!"ACTIVE".equals(streamDescription.getDeliveryStreamStatus())) {
	                waitForStreamToBecomeAvailable(myStreamName);
	            }
	        } catch (ResourceNotFoundException ex) {
	            System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName);

	            // Create a stream. The number of shards determines the provisioned throughput.
	            CreateDeliveryStreamRequest createStreamRequest = new CreateDeliveryStreamRequest();
	            createStreamRequest.setDeliveryStreamName(myStreamName);
	            kinesis.createDeliveryStream(createStreamRequest);
	            // The stream is now being created. Wait for it to become active.
	            waitForStreamToBecomeAvailable(myStreamName);
	        }

	        // List all of my streams.
	        ListDeliveryStreamsRequest listStreamsRequest = new ListDeliveryStreamsRequest();
	        listStreamsRequest.setLimit(10);
	        ListDeliveryStreamsResult listStreamsResult = kinesis.listDeliveryStreams(listStreamsRequest);
	        List<String> streamNames = listStreamsResult.getDeliveryStreamNames();
	        while (listStreamsResult.isHasMoreDeliveryStreams()) {
	            if (streamNames.size() > 0) {
	                listStreamsRequest.setExclusiveStartDeliveryStreamName(streamNames.get(streamNames.size() - 1));
	            }

	            listStreamsResult = kinesis.listDeliveryStreams(listStreamsRequest);
	            streamNames.addAll(listStreamsResult.getDeliveryStreamNames());
	        }
	        // Print all of my streams.
	        System.out.println("List of my streams: ");
	        for (int i = 0; i < streamNames.size(); i++) {
	            System.out.println("\t- " + streamNames.get(i));
	        }

	        System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName);
	        System.out.println("Press CTRL-C to stop.");
	        // Write records to the stream until this program is aborted.
	        File f = new File("/home/impadmin/sampleData/ratings.csv");

	        BufferedReader b = new BufferedReader(new FileReader(f));
	        String readLine = "";

	        while ((readLine = b.readLine()) != null) {
	        	System.out.println(readLine);
	        	 long createTime = System.currentTimeMillis();
	             PutRecordRequest putRecordRequest = new PutRecordRequest();
	             putRecordRequest.setDeliveryStreamName(myStreamName);
	             String data = readLine + "\n";
	 	         Record record = createRecord(data);
	 	         putRecordRequest.setRecord(record);
	             PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
	             System.out.printf("Successfully put record");}
	    }

	    private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
	        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

	        long startTime = System.currentTimeMillis();
	        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
	        while (System.currentTimeMillis() < endTime) {
	            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

	            try {
	                DescribeDeliveryStreamRequest describeStreamRequest = new DescribeDeliveryStreamRequest();
	                describeStreamRequest.setDeliveryStreamName(myStreamName);
	                // ask for no more than 10 shards at a time -- this is an optional parameter
	                describeStreamRequest.setLimit(10);
	                DescribeDeliveryStreamResult describeStreamResponse = kinesis.describeDeliveryStream(describeStreamRequest);

	                String streamStatus = describeStreamResponse.getDeliveryStreamDescription().getDeliveryStreamStatus();
	                System.out.printf("\t- current state: %s\n", streamStatus);
	                if ("ACTIVE".equals(streamStatus)) {
	                    return;
	                }
	            } catch (ResourceNotFoundException ex) {
	                // ResourceNotFound means the stream doesn't exist yet,
	                // so ignore this error and just keep polling.
	            } catch (AmazonServiceException ase) {
	                throw ase;
	            }
	        }

	        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
	    }
	    
	    private static Record createRecord(String data) {
	        return new Record().withData(ByteBuffer.wrap(data.getBytes()));
	}

}
