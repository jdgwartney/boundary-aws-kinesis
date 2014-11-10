/*
 * Copyright 2014 Boundary, Inc. or its affiliates. All Rights Reserved.
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

package com.boundary.aws.kinesis;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class Sample {
	static AmazonKinesisClient kinesisClient;
	private static final Log LOG = LogFactory.getLog(Sample.class);

	private static void init() throws Exception {

		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (HOME/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. ",e);
		}

		kinesisClient = new AmazonKinesisClient(credentials);
		Region region = Region.getRegion(Regions.US_WEST_2);
		kinesisClient.setRegion(region);
	}

	public static void main(String[] args) throws Exception {
		init();

		final String myStreamName = "boundary-test-stream";
		final Integer myStreamSize = 1;

		// Create a stream. The number of shards determines the provisioned
		// throughput.

		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(myStreamName);
		createStreamRequest.setShardCount(myStreamSize);

		kinesisClient.createStream(createStreamRequest);
		// The stream is now being created.
		LOG.info("Creating Stream : " + myStreamName);
		waitForStreamToBecomeAvailable(myStreamName);

		// list all of my streams
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(10);
		ListStreamsResult listStreamsResult = kinesisClient
				.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();
		while (listStreamsResult.isHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames
						.get(streamNames.size() - 1));
			}

			listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());

		}
		LOG.info("Printing my list of streams : ");

		// print all of my streams.
		if (!streamNames.isEmpty()) {
			System.out.println("List of my streams: ");
		}
		for (int i = 0; i < streamNames.size(); i++) {
			System.out.println(streamNames.get(i));
		}

		LOG.info("Putting records in stream : " + myStreamName);
		// Write 10 records to the stream
		for (int j = 0; j < 100; j++) {
			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setStreamName(myStreamName);
			putRecordRequest.setData(ByteBuffer.wrap(String.format(
					"testData-%d", j).getBytes()));
			putRecordRequest.setPartitionKey(String
					.format("partitionKey-%d", j));
			PutRecordResult putRecordResult = kinesisClient
					.putRecord(putRecordRequest);
			System.out.println("Successfully putrecord, partition key : "
					+ putRecordRequest.getPartitionKey() + ", ShardID : "
					+ putRecordResult.getShardId());
		}

		// Delete the stream.
		LOG.info("Deleting stream : " + myStreamName);
		DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
		deleteStreamRequest.setStreamName(myStreamName);

		kinesisClient.deleteStream(deleteStreamRequest);
		// The stream is now being deleted.
		LOG.info("Stream is now being deleted : " + myStreamName);
	}

	private static void waitForStreamToBecomeAvailable(String myStreamName) {

		System.out.println("Waiting for " + myStreamName
				+ " to become ACTIVE...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 20);
			} catch (InterruptedException e) {
				// Ignore interruption (doesn't impact stream creation)
			}
			try {
				DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
				describeStreamRequest.setStreamName(myStreamName);
				// ask for no more than 10 shards at a time -- this is an
				// optional parameter
				describeStreamRequest.setLimit(10);
				DescribeStreamResult describeStreamResponse = kinesisClient
						.describeStream(describeStreamRequest);

				String streamStatus = describeStreamResponse
						.getStreamDescription().getStreamStatus();
				System.out.println("  - current state: " + streamStatus);
				if (streamStatus.equals("ACTIVE")) {
					return;
				}
			} catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equalsIgnoreCase(
						"ResourceNotFoundException") == false) {
					throw ase;
				}
				throw new RuntimeException("Stream " + myStreamName
						+ " never went active");
			}
		}
	}
}
