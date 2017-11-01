package com.amazonaws.samples;
/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;

/**
 * Processes records and checkpoints progress.
 */
public class AmazonKinesisApplicationSampleRecordProcessor implements IRecordProcessor {

	private static final Log LOG = LogFactory.getLog(AmazonKinesisApplicationSampleRecordProcessor.class);
	private String kinesisShardId;

	// Backoff and retry settings
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
	private static final int NUM_RETRIES = 10;
	private static AmazonKinesisFirehoseClient fireHoseClient;
	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;
	private static final String DELIVERY_STREAM_NAME = "DeliveryStream";

	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	//private FileOutputStream out;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(String shardId) {
		/*try {
			File file = new File("/home/impadmin/sampleData/" + shardId);
			if (!file.exists()) {
				if (file.mkdir()) {
					System.out.println("Directory is created!");
				} else {
					System.out.println("Failed to create directory!");
				}
			}
			this.out = new FileOutputStream(file.toString() + "/" + System.currentTimeMillis());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		LOG.info("Initializing record processor for shard: " + shardId);
		this.kinesisShardId = shardId;
		fireHoseClient = new AmazonKinesisFirehoseClient(new ProfileCredentialsProvider("default").getCredentials());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

		// Process records and perform all exception handling.
		processRecordsWithRetries(records);

		// Checkpoint once every checkpoint interval.
		if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
			checkpoint(checkpointer);
			nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
		}
	}

	/**
	 * Process records performing retries as needed. Skip "poison pill" records.
	 * 
	 * @param records
	 *            Data records to be processed.
	 */
	private void processRecordsWithRetries(List<Record> records) {
		boolean processedSuccessfully = false;
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				processRecords(records);
				processedSuccessfully = true;
				break;
			} catch (Throwable t) {
				LOG.warn("Caught throwable while processing records", t);
			}
			// backoff if we encounter an exception.
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				LOG.debug("Interrupted sleep", e);
			}
		}
		if (!processedSuccessfully) {
			LOG.error("Couldn't process records. Skipping the records.");
		}
	}

	private void processRecords(List<Record> records) throws CharacterCodingException {
		Map<Integer, String> movieRatingMap = new HashMap<Integer, String>();
		for (Record record : records) {
			String data = decoder.decode(record.getData()).toString();
			String dataFields[] = data.split(",");
			if (movieRatingMap.get(Integer.parseInt(dataFields[1])) != null) {
				String movieDetails[] = movieRatingMap.get(Integer.parseInt(dataFields[1])).toString().split(",");
				if (Double.parseDouble(dataFields[2]) > Double.parseDouble(movieDetails[0])) {
					movieRatingMap.put(Integer.parseInt(dataFields[1]), dataFields[2] + "," + dataFields[3]);
				}
			} else {
				movieRatingMap.put(Integer.parseInt(dataFields[1]), dataFields[2] + "," + dataFields[3]);
			}
		}
		for (Map.Entry<Integer, String> entry : movieRatingMap.entrySet()) {
			String data = entry.getKey() + "," + entry.getValue() + "\n";
			System.out.println(entry.getKey() + "," + entry.getValue());
			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setDeliveryStreamName(DELIVERY_STREAM_NAME);
			com.amazonaws.services.kinesisfirehose.model.Record record = createRecord(data);
			putRecordRequest.setRecord(record);
			PutRecordResult putRecordResult = fireHoseClient.putRecord(putRecordRequest);
			//String data = entry.getKey() + "," + entry.getValue();
			// out.write(data.getBytes());
		}

	}

	/**
	 * Process a single record.
	 * 
	 * @param record
	 *            The record to be processed.
	 */
	private void processSingleRecord(Record record) {
		// TODO Add your own record processing logic here

		String data = null;
		try {
			// For this app, we interpret the payload as UTF-8 chars.
			data = decoder.decode(record.getData()).toString();
			// Assume this record came from AmazonKinesisSample and log its age.
			System.out.println(data);
		} catch (NumberFormatException e) {
			LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
		} catch (CharacterCodingException e) {
			LOG.error("Malformed data: " + data, e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
		LOG.info("Shutting down record processor for shard: " + kinesisShardId);
		// Important to checkpoint after reaching end of shard, so we can start
		// processing data from child shards.
		if (reason == ShutdownReason.TERMINATE) {
			checkpoint(checkpointer);
		}
		/*try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	/**
	 * Checkpoint with retries.
	 * 
	 * @param checkpointer
	 */
	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Checkpointing shard " + kinesisShardId);
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				checkpointer.checkpoint();
				break;
			} catch (ShutdownException se) {
				// Ignore checkpoint if the processor instance has been shutdown
				// (fail over).
				LOG.info("Caught shutdown exception, skipping checkpoint.", se);
				break;
			} catch (ThrottlingException e) {
				// Backoff and re-attempt checkpoint upon transient failures
				if (i >= (NUM_RETRIES - 1)) {
					LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
					break;
				} else {
					LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
				}
			} catch (InvalidStateException e) {
				// This indicates an issue with the DynamoDB table (check for
				// table, provisioned IOPS).
				LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
				break;
			}
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				LOG.debug("Interrupted sleep", e);
			}
		}
	}

	private static com.amazonaws.services.kinesisfirehose.model.Record createRecord(String data) {
		return new com.amazonaws.services.kinesisfirehose.model.Record().withData(ByteBuffer.wrap(data.getBytes()));
	}
}
