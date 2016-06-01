package com.takipi.tests.speedtest.task;

import com.amazonaws.services.s3.model.Region;

public abstract class UploadTask implements Runnable
{
    protected final Region region;
	protected final String bucket;
	protected final byte[] data;
	
	protected UploadTaskResult result;
	
	public static class UploadTaskResult
	{
		private final boolean 	success;
		private final long 	uploadTime;
		private final long 	uploadMultiPartTime;
		//private final long 	downloadTime;
		
		public UploadTaskResult(boolean success, long uploadTime, long uploadMultiPartTime)
		{
			this.success = success;
			this.uploadTime = uploadTime;
			this.uploadMultiPartTime = uploadMultiPartTime;
		}
		
		public boolean isSuccess()
		{
			return success;
		}

		public long getUploadTime()
		{
			return uploadTime;
		}

		public long getUploadMultiPartTime()
		{
			return uploadMultiPartTime;
		}
	}
	
    public UploadTask(Region region, String bucket, byte[] data)
	{
            this.region = region;
            this.bucket = bucket;
            this.data 	= data;
	}
	
	
	public UploadTaskResult getResult()
	{
		return result;
	}
	
	//@Override
	public abstract void run();
}
