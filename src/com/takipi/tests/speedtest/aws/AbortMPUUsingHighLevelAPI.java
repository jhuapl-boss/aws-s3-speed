package com.takipi.tests.speedtest.aws;

import java.util.Date;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.transfer.TransferManager;

public class AbortMPUUsingHighLevelAPI {

    public static void main(String[] args) throws Exception {

        String existingBucketName = S3Manager.SPEED_TEST_BUCKET;
        
        TransferManager tm = new TransferManager(new ProfileCredentialsProvider());        

        int sevenDays = 1000 * 60 * 60 * 24 * 7;
		Date oneWeekAgo = new Date(System.currentTimeMillis() - sevenDays);
        
        try {
        	tm.abortMultipartUploads(existingBucketName, oneWeekAgo);
        } catch (AmazonClientException amazonClientException) {
        	System.out.println("Unable to upload file, upload was aborted.");
        	amazonClientException.printStackTrace();
        }
    }
}