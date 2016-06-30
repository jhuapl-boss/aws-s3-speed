package com.takipi.tests.speedtest;

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
    private static boolean fullThroughputTest = false;

    
    public static void printUsage() {
        System.out.println("AWS upload speed Test for theboss.io");
        System.out.println("Usage: RUN AWS_KEY AWS_SECRET [MaxThreadPoolSize] [full]");
        System.out.println("   This will perform the speed test and create results file in the same directory");
        System.out.println("   MaxThreadPoolSize - The number of threads to use. Default is 20. Using more threads");
        System.out.println("       can increase performance.  Setting this to high can crash the application.");
        System.out.println("    full - adding the word full will cause the system to perform only the 512MB");
        System.out.println("       multipart upload test.  Running this on on multiple systems help show you the");
        System.out.println("       maximum bandwidth at your site to s3");
        System.out.println("Usage: LIST AWS_KEY AWS_SECRET");
        System.out.println("   Lists all multipart uploads");
        System.out.println("Usage: ABORT AWS_KEY AWS_SECRET");
        System.out.println("   Aborts all multipart uploads older than 7 days");
    }
	
    public static void main(String[] args)
    {
        try {
			if ( !((args.length == 3) || (args.length == 4) || (args.length == 5)) )
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
			int maxThreadpool = 20;	
			if( args.length == 4) {
				if(args[3].toLowerCase().equals("full")) {
					logger.debug("Performing full throughput testings, only use Multipart 512MB tests");
					fullThroughputTest = true;
				} else {
			    	try {
			    	maxThreadpool = Integer.valueOf(args[3]);
			    	} catch (NumberFormatException n) {
			        	System.out.println("Unknown command.");
			        	System.out.println("");
			    		printUsage();
			            return;
			    	}
			    	logger.debug("Setting MaxThreadPool size to " + maxThreadpool);
				}
			} else if( args.length == 5) {
				if(args[4].toLowerCase().equals("full")) {
					logger.debug("Performing full throughput testings, only use Multipart 512MB tests");
					fullThroughputTest = true;
				} 
				try {
				maxThreadpool = Integer.valueOf(args[3]);
				} catch (NumberFormatException n) {
			    	System.out.println("Unknown command.");
			    	System.out.println("");
					printUsage();
			        return;
				}
				logger.debug("Setting MaxThreadPool size to " + maxThreadpool);
			}

			S3Manager.initializeTransferMgr(maxThreadpool);
			S3Manager.initBuckets(false);
			
			if(!fullThroughputTest) {
				sizeTestRound(Size.HUGE);
			}
			sizeTestRound(Size.SUPER);
			S3Manager.shutdownThreadPool();
		} catch (IOException e) {
			logger.debug(e.getMessage());
		} catch (Exception e) {
			logger.debug(e.getMessage());
		}
    }

	private static void sizeTestRound(Size size) throws Exception, IOException {
		String fileName = DataBytes.getData(size);
		
        UploadTaskType uploadType = UploadTaskType.SDK;
        logger.debug("Starting tests for " + DataBytes.getSizeStr(size));
        SpeedTest speedTest = new SpeedTest(ROUNDS, fileName, uploadType, fullThroughputTest);
        speedTest.start();
        logger.debug("Tests finished for " + DataBytes.getSizeStr(size));

        if(!fullThroughputTest) {
        	printResults("Type: SingleUpload, Size: " + DataBytes.getSizeStr(size), 
        		speedTest.getUploadTimings());
        }
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
                	String output = String.format("%s, Region: %s, Timings: %d, Lowest: %d ms, Highest: %d ms, Average: %.0f ms, Median: %.0f ms, MinUploadPartSize: %d MB, MaxThreadPoolSize: %d", 
        			prefixStr, regionName, timingsCount, regionTimings.get(0), regionTimings.get(timingsCount - 1), avg, median, S3Manager.getMinUploadPartSizeMB(), S3Manager.getMaxThreads());
                	FileLogger.info(output, logger);
                }
            }
    }
}
