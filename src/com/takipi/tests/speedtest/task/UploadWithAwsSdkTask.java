package com.takipi.tests.speedtest.task;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.takipi.tests.speedtest.aws.S3Manager;
import com.takipi.tests.speedtest.aws.UploadObjectMPULowLevelAPI;
import com.amazonaws.services.s3.model.Region;

public class UploadWithAwsSdkTask extends UploadTask
{
	private static final Logger logger = LoggerFactory.getLogger(UploadWithAwsSdkTask.class);
	private long uploadTime =  0;
	private boolean success = false;
	
    public UploadWithAwsSdkTask(Region region, String bucket, String fileName)
	{
            super(region, bucket, fileName);
	}
    
    public void performMultiPartUploadTest() throws IOException {
		String key = UUID.randomUUID().toString();
		
		long start = System.currentTimeMillis();
		boolean successMultiPart = S3Manager.putFile(region, bucket, key, fileName, true);
		long finish = System.currentTimeMillis();

		long uploadMultiPartTime = finish - start;
		
		logger.debug("MultiPartUpload task to {} finished in {} ms", bucket, uploadMultiPartTime);
		result = new UploadTaskResult(success && successMultiPart, uploadTime, uploadMultiPartTime);

		S3Manager.deleteBytes(region, bucket, key);

    }
	
	@Override
	public void run()
	{
		String key = UUID.randomUUID().toString();
		
		long start = System.currentTimeMillis();
		
		try {
			success = S3Manager.putFile(region, bucket, key, fileName, false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long finish = System.currentTimeMillis();
		
		uploadTime = finish - start;
		
		logger.debug("Upload task to bucket {} finished in {} ms", bucket, uploadTime);

		if (!success) {
			result = new UploadTaskResult(success, uploadTime, 0);
			return;
		}

		S3Manager.deleteBytes(region, bucket, key);
	}
}
