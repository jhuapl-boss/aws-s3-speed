package com.takipi.tests.speedtest.aws;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class MultipartUploadUtil {

	private	AWSCredentials creds;
    private static final Logger logger = LoggerFactory.getLogger(S3Manager.class);

	
	public MultipartUploadUtil(AWSCredentials credentials) {
		creds = credentials;
	}
	
	/**
	 * Aborts all multipart uploads older than 7 days
	 * @param bucketName
	 */
	public void abortMultipartUploads(String bucketName) {
        TransferManager tm = new TransferManager(creds);        

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
	
	/**
	 * uploads file to bucket using multipartupload java low level sdk
	 * @param existingBucketName
	 * @param keyName
	 * @param filePath
	 * @return
	 */
	public boolean multiPartUpload(String existingBucketName, String keyName, String filePath) {
        AmazonS3 s3Client = new AmazonS3Client(CredentialsManager.getCreds());        

        // Create a list of UploadPartResponse objects. You get one of these
        // for each part upload.
        List<PartETag> partETags = new ArrayList<PartETag>();

        // Step 1: Initialize.
        InitiateMultipartUploadRequest initRequest = new 
             InitiateMultipartUploadRequest(existingBucketName, keyName);
        InitiateMultipartUploadResult initResponse = 
        	                   s3Client.initiateMultipartUpload(initRequest);

        File file = new File(filePath);
        long contentLength = file.length();
        long partSize = 5242880*4; // Set part size to 5 MB.

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be less than 5 MB. Adjust part size.
            	partSize = Math.min(partSize, (contentLength - filePosition));
            	
            	logger.debug("creating part " + i);
                // Create request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(existingBucketName).withKey(keyName)
                    .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                    .withFileOffset(filePosition)
                    .withFile(file)
                    .withPartSize(partSize);

                // Upload part and add response to our list.
                partETags.add(
                		s3Client.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new 
                         CompleteMultipartUploadRequest(
                                    existingBucketName, 
                                    keyName, 
                                    initResponse.getUploadId(), 
                                    partETags);

            s3Client.completeMultipartUpload(compRequest);
            return true;
        } catch (Exception e) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                    existingBucketName, keyName, initResponse.getUploadId()));
            return false;
        }
		
	}

}
