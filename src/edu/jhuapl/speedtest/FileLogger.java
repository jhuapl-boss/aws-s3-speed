package edu.jhuapl.speedtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.takipi.tests.speedtest.Main;


/**
 * This is a quick workaround for logback which doesn't have a way to log to two different 
 * levels with a different output for each one. 
 * @author Sandy Hider
 *
 */
public class FileLogger {

    private static final Logger logger = LoggerFactory.getLogger(FileLogger.class);
    
    public static void info(String str, Logger logger2) {
    	logger.info(str);
    	logger2.info(str);
    }

}
