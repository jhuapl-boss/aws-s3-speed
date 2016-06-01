package com.takipi.tests.speedtest;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.Region;
import com.takipi.tests.speedtest.aws.CredentialsManager;
import com.takipi.tests.speedtest.aws.S3Manager;
import com.takipi.tests.speedtest.data.DataBytes;
import com.takipi.tests.speedtest.data.DataBytes.Size;
import com.takipi.tests.speedtest.task.UploadTaskType;
import edu.jhuapl.speedtest.FileLogger;

public class Main
{
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
	
    public static void main(String[] args) throws Exception
    {
        if (!(args.length == 2))
            {
                System.out.println("AWS upload speed - By Takipi");
                System.out.println("Usage: AWS_KEY AWS_SECRET");
                return;
            }
		File f = new File(".");
		logger.debug("path: " + f.getAbsolutePath());
        CredentialsManager.setup(args[0], args[1]);
        S3Manager.initBuckets(false);
		
        int rounds = 1;
        Size size = Size.HUGE;
        byte[] data = DataBytes.getData(size);
		
        UploadTaskType uploadType = UploadTaskType.SDK;
		
        logger.debug("Starting test");
		
        SpeedTest speedTest = new SpeedTest(rounds, data, uploadType);
        speedTest.start();
        
		
        logger.debug("Test finished");

        printResults("Upload results " + DataBytes.getSizeStr(size) + ": ", 
        		speedTest.getUploadTimings());
        printResults("MultiPartUpload results " + DataBytes.getSizeStr(size) + ": ",
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
                logger.debug("RegionName: '{}'", regionName);
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
                	String output = String.format("%s Region %s: %d valid tasks. lowest: %d ms, highest: %d ms. Average: %.0f ms, median: %.0f ms.", 
        			prefixStr, regionName, timingsCount, regionTimings.get(0), regionTimings.get(timingsCount - 1), avg, median);
                	FileLogger.info(output, logger);
                }
            }
    }
}
