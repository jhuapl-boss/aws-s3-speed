package com.takipi.tests.speedtest.aws;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.identitymanagement.model.transform.GetCredentialReportResultStaxUnmarshaller;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3Manager
{
    private static String BUCKET_PREFIX;// = "takipi-aws-speed-test-";
    private static String BUCKET_SUFFIX;// = "-05-mar-2013";
	
    private static final Logger logger = LoggerFactory.getLogger(S3Manager.class);

    private static final AmazonS3 s3client;
    private static final TransferManager txMgr;

    public static final Map<Region, String> buckets;
    public static final String SPEED_TEST_BUCKET = "theboss.io-speed-test";
    private static long totalbytes = 0;
	
    static
    {
        s3client = new AmazonS3Client(CredentialsManager.getCreds());
        buckets = new HashMap<Region, String>();
        txMgr = new TransferManager(CredentialsManager.getCreds());
    }
	
    public static void initBuckets(String prefix, String suffix)
    {
        BUCKET_PREFIX = prefix;
        BUCKET_SUFFIX = suffix;
    }
	
    public static Map<Region, String> getBuckets()
    {
        return buckets;
    }
	
    public static void initBuckets(boolean create)
    {
        String regionName = "";
//        for (Region region : Region.values())
            {
            	Region region = Region.US_Standard;
            	regionName = "us-east-1";
//                logger.debug("Region: '{}'", region);
//                
//                if (region.toString() != null) {
//                    regionName = region.toString();
//                } else {
//                    regionName = "us-east-1";
//                }
//                logger.debug("RegionName: '{}'", regionName);
//                System.out.println("RegionName: " + regionName);
//                
//                // need to skip this region because we're not authorized
//                if (regionName.equals("s3-us-gov-west-1") || regionName.equals("cn-north-1")) {
//                    logger.debug("Skipping: Not authorized for region {}", regionName);
//                    continue;
//                }
//                    
//                StringBuilder bucketNameBuilder = new StringBuilder();
//                bucketNameBuilder.append(BUCKET_PREFIX);
//                bucketNameBuilder.append(regionName);
//                bucketNameBuilder.append(BUCKET_SUFFIX);
//			
//                String bucketName = bucketNameBuilder.toString().toLowerCase();
                String bucketName = SPEED_TEST_BUCKET;
                    
                buckets.put(region, bucketName);
			
                if (create)
                    {
                        try {                            
                            // need to set the region for "eu-central-1" region to work
                            // this enables V4 signing
                            // careful, this is not thread-safe!
                            logger.debug("Setregion: {}", regionName);
                            s3client.setRegion(RegionUtils.getRegion(regionName)); 
                            if (! s3client.doesBucketExist(bucketName)) {
                                s3client.createBucket(bucketName, region);
                                logger.debug("Creating bucket {} in region {}", bucketName, region);
                            } else
                                logger.debug("Skipping: Bucket {} in region {} already exists.", bucketName, region);
                            
                        } catch (AmazonServiceException ase) {
                            logger.debug("Caught an AmazonServiceException, which " +
                                         "means your request made it " +
                                         "to Amazon S3, but was rejected with an error response" +
                                         " for some reason.");
                            logger.debug("Error Message:    " + ase.getMessage());
                            logger.debug("HTTP Status Code: " + ase.getStatusCode());
                            logger.debug("AWS Error Code:   " + ase.getErrorCode());
                            logger.debug("Error Type:       " + ase.getErrorType());
                            logger.debug("Request ID:       " + ase.getRequestId());
                        } catch (AmazonClientException ace) {
                            logger.debug("Caught an AmazonClientException, which " +
                                         "means the client encountered " +
                                         "an internal error while trying to " +
                                         "communicate with S3, " +
                                         "such as not being able to access the network.");
                            logger.debug("Error Message: " + ace.getMessage());
                        }
                    }  // comment out to here to remove create

            }
    }
	
    public static void removeBuckets()
    {
        for (String bucketName : buckets.values())
            {
                s3client.deleteBucket(bucketName);
			
                logger.debug("Deleting bucket {}", bucketName);
            }
    }
	
    public static URL getSignedUrl(String bucket, String key, HttpMethod method)
    {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, key, method);
		
        return s3client.generatePresignedUrl(request);

    }

    public static boolean putBytes(Region region, String bucket, String key, byte[] bytes, boolean multiPart)
    {
        ObjectMetadata metaData = new ObjectMetadata();
        metaData.setContentLength(bytes.length);
        if( multiPart)
        	return doPutObjectMultiPart(region, bucket, key, new ByteArrayInputStream(bytes), metaData);
        else 
        	return doPutObject(region, bucket, key, new ByteArrayInputStream(bytes), metaData);
    }
    
    public static byte[] getBytes(Region region, String bucket, String key)
    {
        return doGetObject(region, bucket, key);
    }

    public static void deleteBytes(Region region, String bucket, String key)
    {
        doDeleteObject(region, bucket, key);
    }

    private static boolean doPutObject(Region region, String bucket, String key, InputStream is, ObjectMetadata metaData)
    {
        try
            {
                String regionName = "";
                if (region.toString() != null) {
                    regionName = region.toString();
                } else {
                    regionName = "us-east-1";
                }
                logger.debug("Setregion: {}", regionName);
                // need to set the region for "eu-central-1" region to work
                // this enables V4 signing
                // careful, this is not thread-safe!
                s3client.setRegion(RegionUtils.getRegion(regionName));
                logger.debug("PUT object to S3 bucket: {}", bucket);
                s3client.putObject(bucket, key, is, metaData);
                return true;
            }
        catch (Exception e)
            {
                logger.error("Error putting object", e);
                return false;
            }
    }

    private static boolean doPutObjectMultiPart(Region region, String bucket, String key, InputStream is, ObjectMetadata metaData)
    {
    	TransferManagerConfiguration tmc = new TransferManagerConfiguration();
    	tmc.setMinimumUploadPartSize(15000000);
        TransferManager tm = new TransferManager( CredentialsManager.getCreds()); //new DefaultAWSCredentialsProviderChain());        
        tm.setConfiguration(tmc);
        
        // For more advanced uploads, you can create a request object 
        // and supply additional request parameters (ex: progress listeners,
        // canned ACLs, etc.)
        PutObjectRequest request = new PutObjectRequest(
        		bucket, key, is, metaData);

        // You can ask the upload for its progress, or you can 
        // add a ProgressListener to your request to receive notifications 
        // when bytes are transferred.
        
//        request.setGeneralProgressListener(new ProgressListener() {
//			public void progressChanged(ProgressEvent progressEvent) {
//				long bytesTransferred = S3Manager.addBytes(progressEvent.getBytesTransferred());
//				//if(bytesTransferred % (1024*1024) == 0)
//					logger.debug("Transferred bytes: " + bytesTransferred);
//				
//							
//			}
//		});

        // TransferManager processes all transfers asynchronously, 
        // so this call will return immediately.
        Upload upload = tm.upload(request);

        try {
        	// You can block and wait for the upload to finish
        	logger.debug("about to wait for completion");
        	upload.waitForCompletion();
        	//UploadResult upresult = upload.waitForUploadResult();
        	logger.debug("finished waiting for completion");
        	//upload.abort();
        	return true;
        } catch (AmazonClientException amazonClientException) {
        	System.out.println("Unable to upload file, upload aborted.");
        	amazonClientException.printStackTrace();
        	return false;
        } catch (InterruptedException e) {
        	logger.debug("Multipart upload was interrupted.");
			e.printStackTrace();
			return false;
		} finally {
			try {
			if(tm!=null)
				tm.shutdownNow();
			} catch (Exception e) {			
			}
		}
    	
//    	DefaultAWSCredentialsProviderChain credentialProviderChain = new DefaultAWSCredentialsProviderChain();
//    	TransferManager tx = new TransferManager(
//    	               credentialProviderChain.getCredentials());
//    	Upload myUpload = tx.upload(bucket, key, is, metaData);
//    	 
//    	// You can poll your transfer's status to check its progress
//    	if (myUpload.isDone() == false) {
//    	       System.out.println("Transfer: " + myUpload.getDescription());
//    	       System.out.println("  - State: " + myUpload.getState());
//    	       System.out.println("  - Progress: "
//    	                       + myUpload.getProgress().getBytesTransferred());
//    	}
//    	 
//    	// Transfers also allow you to set a <code>ProgressListener</code> to receive
//    	// asynchronous notifications about your transfer's progress.
//    	myUpload.addProgressListener(myProgressListener);
//    	 
//    	// Or you can block the current thread and wait for your transfer to
//    	// to complete. If the transfer fails, this method will throw an
//    	// AmazonClientException or AmazonServiceException detailing the reason.
//    	myUpload.waitForCompletion();
//    	 
//    	// After the upload is complete, call shutdownNow to release the resources.
//    	tx.shutdownNow();    	
//    	
//        try
//            {
//                String regionName = "";
//                if (region.toString() != null) {
//                    regionName = region.toString();
//                } else {
//                    regionName = "us-east-1";
//                }
//                logger.debug("Setregion: {}", regionName);
//                // need to set the region for "eu-central-1" region to work
//                // this enables V4 signing
//                // careful, this is not thread-safe!
//                s3client.setRegion(RegionUtils.getRegion(regionName));
//                logger.debug("PUT object to S3 bucket: {}", bucket);
//                s3client.putObject(bucket, key, is, metaData);
//                return true;
//            }
//        catch (Exception e)
//            {
//                logger.error("Error putting object", e);
//                return false;
//            }
        
    }

    private static byte[] doGetObject(Region region, String bucket, String key)
    {
        try
        {
            String regionName = "";
            if (region.toString() != null) {
                regionName = region.toString();
            } else {
                regionName = "us-east-1";
            }
            logger.debug("Setregion: {}", regionName);
            // need to set the region for "eu-central-1" region to work
            // this enables V4 signing
            // careful, this is not thread-safe!
            s3client.setRegion(RegionUtils.getRegion(regionName));
            logger.debug("GET object from S3 bucket: {}", bucket);
            S3Object object = s3client.getObject(bucket, key);
            InputStream reader = new BufferedInputStream(object.getObjectContent());
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            OutputStream writer = new BufferedOutputStream(bytes);
            int read = -1;
            while ( ( read = reader.read() ) != -1 ) {
                writer.write(read);
            }
            writer.flush();
            writer.close();
            reader.close();
            object.close();
            byte[] data = bytes.toByteArray();
            bytes.close();
            return data;            
        }
        catch (Exception e)
        {
            logger.error("Error getting object", e);
            return null;
        }
    }

    private static void doDeleteObject(Region region, String bucket, String key)
    {
        try
        {
            String regionName = "";
            if (region.toString() != null) {
                regionName = region.toString();
            } else {
                regionName = "us-east-1";
            }
            logger.debug("Setregion: {}", regionName);
            // need to set the region for "eu-central-1" region to work
            // this enables V4 signing
            // careful, this is not thread-safe!
            s3client.setRegion(RegionUtils.getRegion(regionName));
            logger.debug("DELETE object from S3 bucket: {}", bucket);
            s3client.deleteObject(bucket, key);
        }
        catch (Exception e)
        {
            logger.error("Error deleting object", e);
            return;
        }
    }
    
    public static long addBytes(long bytes) {
    	totalbytes += bytes;
    	return totalbytes;
    }
    
    public static void clearBytes() {
    	totalbytes =0L;
    }

}
