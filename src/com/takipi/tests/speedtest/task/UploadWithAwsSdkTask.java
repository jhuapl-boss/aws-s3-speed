package com.takipi.tests.speedtest.task;

import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.takipi.tests.speedtest.aws.S3Manager;
import com.amazonaws.services.s3.model.Region;

public class UploadWithAwsSdkTask extends UploadTask
{
	private static final Logger logger = LoggerFactory.getLogger(UploadWithAwsSdkTask.class);
	private long uploadTime =  0;
	private boolean success = false;
	
    public UploadWithAwsSdkTask(Region region, String bucket, byte[] data)
	{
            super(region, bucket, data);
	}
    
    public void performMultiPartUploadTest() {
		String key = UUID.randomUUID().toString();
		
		long start = System.currentTimeMillis();

		boolean successMultiPart = S3Manager.putBytes(region, bucket, key, data, true);

		long finish = System.currentTimeMillis();

		long uploadMultiPartTime = finish - start;
		
		logger.debug("UploadMultiPast task to {} finished in {} ms", bucket, uploadMultiPartTime);

		result = new UploadTaskResult(success && successMultiPart, uploadTime, uploadMultiPartTime);

		S3Manager.deleteBytes(region, bucket, key);

		logger.debug("Delete task to {} finished", bucket);
    }
	
	@Override
	public void run()
	{
		String key = UUID.randomUUID().toString();
		
		long start = System.currentTimeMillis();
		
		success = S3Manager.putBytes(region, bucket, key, data, false);
		
		long finish = System.currentTimeMillis();
		
		uploadTime = finish - start;
		
		logger.debug("Upload task to bucket {} finished in {} ms", bucket, uploadTime);

		if (!success) {
			result = new UploadTaskResult(success, uploadTime, 0);
			return;
		}

		S3Manager.deleteBytes(region, bucket, key);
		logger.debug("Delete task to {} finished", bucket);
	}
}
