package com.takipi.tests.speedtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.Region;
import com.takipi.tests.speedtest.aws.S3Manager;
import com.takipi.tests.speedtest.task.UploadTaskType;
import com.takipi.tests.speedtest.task.UploadWithAwsSdkTask;
import com.takipi.tests.speedtest.task.UploadTask.UploadTaskResult;

public class SpeedTest
{
	private static final Logger logger = LoggerFactory.getLogger(SpeedTest.class);
	
	private final int rounds;
	//private final byte[] data;
	private final String fileName;
	private final UploadTaskType uploadType;
	
	private final Map<Region, List<Long>> uploadTimings;
	private final Map<Region, List<Long>> uploadMutiPartTimings;
	
    private boolean fullThroughputTest = false;

	
	public SpeedTest(int rounds, String fileName, UploadTaskType uploadType, boolean fullThroughputTest)
	{
		this.rounds = rounds;
		this.fileName = fileName;
		this.uploadType = uploadType;
		this.fullThroughputTest = fullThroughputTest;
		
		this.uploadTimings = new HashMap<Region, List<Long>>();
		this.uploadMutiPartTimings = new HashMap<Region, List<Long>>();
	}
	
	public void start() throws IOException
	{
		init();
		
		Map<Region, String> buckets = S3Manager.getBuckets();
		
		for (int i = 0; i < rounds; i++)
		{
			logger.debug("*** Round {}/{} ***", i+1, rounds);
			
			List<Region> regions = new ArrayList<Region>(buckets.keySet());
			Collections.shuffle(regions);
			
			for (Region region : regions)
			{
//				logger.debug("About to upload in region {}", region);
				
				UploadWithAwsSdkTask uploadTask;
				uploadTask = new UploadWithAwsSdkTask(region,buckets.get(region), fileName);
				if(!fullThroughputTest) {
					uploadTask.run();
				} else {
					uploadTask.setSuccess(true);
				}
				uploadTask.performMultiPartUploadTest();
				
				UploadTaskResult result = uploadTask.getResult();
				
				if ((result != null) && (result.isSuccess()))
				{
					uploadTimings.get(region).add(Long.valueOf(result.getUploadTime()));
					uploadMutiPartTimings.get(region).add(Long.valueOf(result.getUploadMultiPartTime()));
				}
			}
		}
	}

	public Map<Region, List<Long>> getUploadTimings()
	{
		return uploadTimings;
	}

	public Map<Region, List<Long>> getMultiPartUploadTimings()
	{
		return uploadMutiPartTimings;
	}
	
	private void init()
	{
		Region region = Region.US_Standard;
		List<Long> list = new ArrayList<Long>();
		List<Long> list2 = new ArrayList<Long>();
		this.uploadTimings.put(region, list);
		this.uploadMutiPartTimings.put(region, list2);

//		for (Region region : Region.values())
//		{
//			List<Long> list = new ArrayList<Long>();
//			this.uploadTimings.put(region, list);
//			this.downloadTimings.put(region, list);
//		}
		
		S3Manager.initBuckets(false);
	}
	
	public boolean isFullThroughputTest() {
		return fullThroughputTest;
	}

	public void setFullThroughputTest(boolean fullThroughputTest) {
		this.fullThroughputTest = fullThroughputTest;
	}
}
