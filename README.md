Theboss.io Speed Test
======================

This project is used to create a java jar file for testing theboss.io bandwidth.  It uses open source code from www.takipi.com:  aws-s3-speed.

APL has modified the code to run a series of tests using 100MB and 512MB files uploading them using Single Upload and MultiPart Upload methods.


Installation
------------
a. Fork / Get the code and run<br/>
```mvn clean compile assembly:single```<br/>
b. Download latest snapshot<br/>
https://s3.amazonaws.com/theboss.io-download/speed-test/theboss.io-speed-test-jar-with-dependencies.jar
  
Usage
-----
You will need your AWS access keys.<br/>
The speedtest uses S3 bucket theboss.io-speed-test in the East Standard Region<br/>

Running the test:<br/>
```java -jar theboss.io-speed-test-jar-with-dependencies.jar RUN AWS_KEY AWS_SECRET```

The test will download 100MB.zip and 512MB.zip files and use those files for uploading, testing each file 24 times:  12 times using single upload and 12 times using mutipart upload techniques.  The high and low values in each category will be thrown out and the remaining 10 will be averaged.

The results will be recorded in the current directory in the file named: theboss.io-speed-test-results.txt

There are a few other commands that are possible:
```java -jar theboss.io-speed-test-jar-with-dependencies.jar LIST AWS_KEY AWS_SECRET```
This will list all the multipart uploads under the S3 bucket theboss.io-speed-test.

```java -jar theboss.io-speed-test-jar-with-dependencies.jar ABORT AWS_KEY AWS_SECRET```
This will abort all the multipart uploads older then a week in the S3 bucket theboss.io-speed-test.

The follow other options are not documented but can be run.
```java -jar theboss.io-speed-test-jar-with-dependencies.jar RUN AWS_KEY AWS_SECRET X```
Where X is a number representing the minimum multipart upload chunk size.  The default is 5, but can be tested with other numbers 10, 15, 20...
 
 ```java -jar theboss.io-speed-test-jar-with-dependencies.jar RUN AWS_KEY AWS_SECRET full```
 This option will only run the 512MB Multipart upload tests.  It was created to test the full saturation point by running this in multiple shells and on multiple computers.
  
