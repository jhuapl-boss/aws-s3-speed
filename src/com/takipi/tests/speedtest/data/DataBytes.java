package com.takipi.tests.speedtest.data;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.Path;

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
		MEDIUM, BIG, HUGE, SUPER
	}

	public static String getData(Size size) throws Exception
	{
		switch (size)
		{
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


	private static String get5MBData() throws Exception
	{
		return download("5MB.zip");
	}

	private static String get10MBData() throws Exception
	{
		return download("10MB.zip");
	}
	
	private static String get100MBData() throws Exception
	{
		return download("100MB.zip");
	}

	private static String get500MBData() throws Exception
	{
		return download("512MB.zip");
	}
	

	
	private static String download(String file) throws Exception
	{	
		Path path = Paths.get(file);
		if(Files.exists(path)) {
			logger.debug("Using local copy of " + file + ".");
			return file;
		} else {
			if( file.equals("100MB.zip") || file.equals("512MB.zip"))
				logger.debug("Starting to download of " + file +
						" file. This could take a while.");
			else
				logger.debug("Starting to download of " + file + ".");
			URL url = new URL("http", URL, "/" + file);
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			httpConn.addRequestProperty("User-Agent", USER_AGENT);
			InputStream in = httpConn.getInputStream();
			FileOutputStream out = new FileOutputStream(file);
			logger.debug("Caching file " + file + " locally.");
			IOUtils.copy(in, out);
			out.close();
			in.close();
			return file;
		}
	}
}
