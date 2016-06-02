package com.takipi.tests.speedtest;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.Region;
import com.takipi.tests.speedtest.aws.CredentialsManager;
import com.takipi.tests.speedtest.aws.MultipartUploadUtil;
import com.takipi.tests.speedtest.aws.S3Manager;
import com.takipi.tests.speedtest.data.DataBytes;
import com.takipi.tests.speedtest.data.DataBytes.Size;
import com.takipi.tests.speedtest.task.UploadTaskType;
import edu.jhuapl.speedtest.FileLogger;

public class Main
{
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final int ROUNDS = 12;
    
    public static void printUsage() {
        System.out.println("AWS upload speed Test for theboss.io");
        System.out.println("Usage: RUN AWS_KEY AWS_SECRET");
        System.out.println("   This will perform the speed test and create results file");
        System.out.println("   in the same directory");
        System.out.println("Usage: LIST AWS_KEY AWS_SECRET");
        System.out.println("   Lists all multipart uploads");
        System.out.println("Usage: ABORT AWS_KEY AWS_SECRET");
        System.out.println("   Aborts all multipart uploads older than 7 days");
    }
	
    public static void main(String[] args) throws Exception
    {
        if (!(args.length == 3))
            {
        		printUsage();
                return;
            }
        CredentialsManager.setup(args[1], args[2]);
        
        if(args[0].toLowerCase().equals("list")) {
        	MultipartUploadUtil mpuUtil = new MultipartUploadUtil(CredentialsManager.getCreds());
        	mpuUtil.listMultipartUploads(S3Manager.SPEED_TEST_BUCKET);
        	return;
        } else if(args[0].toLowerCase().equals("abort")) {
        	MultipartUploadUtil mpuUtil = new MultipartUploadUtil(CredentialsManager.getCreds());
        	mpuUtil.abortMultipartUploads(S3Manager.SPEED_TEST_BUCKET);
        	return;
        } else if(!args[0].toLowerCase().equals("run")) {
        	System.out.println("Unknown command.");
        	System.out.println("");
    		printUsage();
            return;
        }
        
        S3Manager.initBuckets(false);
		
        sizeTestRound(Size.HUGE);
        sizeTestRound(Size.SUPER);
    }

	private static void sizeTestRound(Size size) throws Exception, IOException {
		String fileName = DataBytes.getData(size);
		
        UploadTaskType uploadType = UploadTaskType.SDK;
        logger.debug("Starting tests for " + DataBytes.getSizeStr(size));
        SpeedTest speedTest = new SpeedTest(ROUNDS, fileName, uploadType);
        speedTest.start();
        logger.debug("Tests finished for " + DataBytes.getSizeStr(size));

        printResults("Type: SingleUpload, Size: " + DataBytes.getSizeStr(size), 
        		speedTest.getUploadTimings());
        printResults("Type: MultiPartUpload, Size: " + DataBytes.getSizeStr(size),
        		speedTest.getMultiPartUploadTimings());
	}
	
    private static void printResults(String prefixStr, Map<Region, List<Long>> timings)
    {
    	// Adjusted to only use US_Standard instead of all regions.
        String regionName = "";
        
//        for (Region region : Region.values())
            {
            	Region region = Region.US_Standard;
            	
                if (region.toString() != null) {
                    regionName = region.toString();
                } else {
                    regionName = "us-east-1";
                }
                //logger.debug("RegionName: '{}'", regionName);
//                if (regionName.equals("s3-us-gov-west-1") || regionName.equals("cn-north-1")) {
//                    logger.debug("Skipping: Not authorized for region {}", regionName);
//                    continue;
//                }

                long sum = 0;
			
                List<Long> regionTimings = timings.get(region);

                if (regionTimings.size() == 0) {
                    logger.debug("Skipping: No results for region {}", regionName);
//                    continue;
                }
			
                if (regionTimings.size() > 1)
                    {
                        Collections.sort(regionTimings);
                    }

                if (regionTimings.size() > 2)
                    {
                        regionTimings.remove(0);
                        regionTimings.remove(regionTimings.size() - 1);
                    }
			
                int timingsCount = regionTimings.size();
			
                for (Long time : timings.get(region))
                    {
                        sum += time.longValue();
                    }
			
                double avg = sum / (double)timingsCount;
                double median;
                if( regionTimings.size() == 1)
                {
                	median = sum;
                } 
                else 
                {
                    int middle = timingsCount / 2;
                    if (timingsCount == 1)
                        {
                            median = regionTimings.get(0);
                        }
                    else if (timingsCount % 2 == 1)
                        {
                            median = regionTimings.get(middle);
                        }
                    else
                    {
                        median = (regionTimings.get(middle - 1) + regionTimings.get(middle)) / 2.0;
                    }
                }
                if(regionTimings.isEmpty()) {
                	FileLogger.info(prefixStr + "Region " + regionName + ": has no timings.", logger);
                }
                else {
                	String output = String.format("%s, Region: %s, Timings: %d, Lowest: %d ms, Highest: %d ms, Average: %.0f ms, Median: %.0f ms", 
        			prefixStr, regionName, timingsCount, regionTimings.get(0), regionTimings.get(timingsCount - 1), avg, median);
                	FileLogger.info(output, logger);
                }
            }
    }
}
