package com.takipi.tests.speedtest.aws;

import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

public class MultipartUploadUtil {

	private	AWSCredentials creds;

	
	public MultipartUploadUtil(AWSCredentials credentials) {
		creds = credentials;
	}
	
	/**
	 * Aborts all multipart uploads older than 7 days
	 * @param bucketName
	 */
	public void abortMultipartUploads(String bucketName) {
        TransferManager tm = new TransferManager(CredentialsManager.getCreds());        

        int sevenDays = 1000 * 60 * 60 * 24 * 7;
		Date oneWeekAgo = new Date(System.currentTimeMillis() - sevenDays);
        
        try {
        	tm.abortMultipartUploads(bucketName, oneWeekAgo);
        } catch (AmazonClientException amazonClientException) {
        	System.out.println("Unable to upload file, upload was aborted.");
        	amazonClientException.printStackTrace();
        }
	}
	
	/**
	 * list all MultipartUploads for specified bucket, providing key, owner and creation date.
	 * @param bucketName
	 */
	public void listMultipartUploads(String bucketName) {
		AmazonS3Client s3Client = new AmazonS3Client(creds);
		ListMultipartUploadsRequest allMultpartUploadsRequest = 
			     new ListMultipartUploadsRequest(bucketName);
			MultipartUploadListing multipartUploadListing = 
			     s3Client.listMultipartUploads(allMultpartUploadsRequest);
			List<MultipartUpload> list = multipartUploadListing.getMultipartUploads();
			if(list.isEmpty()){
				System.out.println("No multipart uploads exist.");
				return;
			}
			for(MultipartUpload a : list) {
				System.out.println("Key: " + a.getKey());
				System.out.println("Owner: " + a.getOwner());
				System.out.println("Date: " + a.getInitiated());
				System.out.println("");
			}
	}

}
