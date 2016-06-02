package com.takipi.tests.speedtest.data;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBytes
{
	private final static String URL = "ipv4.download.thinkbroadband.com";
	private final static String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36";
    private static final Logger logger = LoggerFactory.getLogger(DataBytes.class);


	public static enum Size
	{
		SMALL, MEDIUM, BIG, HUGE, SUPER
	}

	public static byte[] getData(Size size) throws Exception
	{
		switch (size)
		{
			case SMALL:
				return get1KBData();
			case MEDIUM:
				return get5MBData();
			case BIG:
				return get10MBData();
			case HUGE:
				return get100MBData();
			case SUPER:
				return get500MBData();
		}
		
		return null;
	}
	
	public static String getSizeStr(Size size) throws Exception
	{
		switch (size)
		{
			case SMALL:
				return "1KB";
			case MEDIUM:
				return "5MB";
			case BIG:
				return "10MB";
			case HUGE:
				return "100MB";
			case SUPER:
				return "500MB";
		}
		return null;
	}


	private static byte[] get1KBData() throws Exception
	{
		byte[] bytes = new byte[1024];
		Random random = new Random();
		random.nextBytes(bytes);

		return bytes;
	}

	private static byte[] get5MBData() throws Exception
	{
        logger.info("Starting to download 5MB file.");
		return download("5MB.zip");
	}

	private static byte[] get10MBData() throws Exception
	{
        logger.info("Starting to download 10MB file.");
		return download("10MB.zip");
	}
	
	private static byte[] get100MBData() throws Exception
	{
        logger.info("Starting to download 100MB file. This could take a while.");
		return download("100MB.zip");
	}

	private static byte[] get500MBData() throws Exception
	{
        logger.info("Starting to download 512MB file. This could take a while.");
		return download("512MB.zip");
	}

	
	private static byte[] download(String file) throws Exception
	{	// TODO put back http download
		URL url = new URL("file:///home/hiderrt1/Downloads/100MB.zip");
		@SuppressWarnings("restriction")
		sun.net.www.protocol.file.FileURLConnection httpConn = (sun.net.www.protocol.file.FileURLConnection) url.openConnection();
		
//		URL url = new URL("http", URL, "/" + file);
//		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
//		httpConn.addRequestProperty("User-Agent", USER_AGENT);
		
		@SuppressWarnings("restriction")
		InputStream in = httpConn.getInputStream();
		return IOUtils.toByteArray(in);
	}
}
