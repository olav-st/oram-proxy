package eoram.cloudexp.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import eoram.cloudexp.artifacts.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements utility functions for interacting with Amazon S3.
 */
public class AmazonS3Utils 
{
	protected class CredentialsStats
	{
		protected String accessKey = null;
		protected String secretKey = null;
		protected String endpointUrl = null;
		protected String region = null;
	}
	
	private static final AmazonS3Utils instance = new AmazonS3Utils();
	
	private AmazonS3Utils() {}
	
	public static AmazonS3Utils getInstance() { return instance; }

	public void processASE(AmazonServiceException ase)
	{
		System.out.println("Caught an AmazonServiceException, which means your request made it "
				+ "to Amazon S3, but was rejected with an error response for some reason.");
		System.out.println("Error Message:    " + ase.getMessage());
		System.out.println("HTTP Status Code: " + ase.getStatusCode());
		System.out.println("AWS Error Code:   " + ase.getErrorCode());
		System.out.println("Error Type:       " + ase.getErrorType());
		System.out.println("Request ID:       " + ase.getRequestId());
		
		Errors.error(ase);
	}
	
	public void processACE(AmazonClientException ace)
	{
		System.out.println("Caught an AmazonClientException, which means the client encountered "
                + "a serious internal problem while trying to communicate with S3, "
                + "such as not being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
        
        Errors.error(ace);
	}
	
	private CredentialsStats parseCredentials(File credentials)
	{
		CredentialsStats ret = new CredentialsStats();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(credentials));
			ret.accessKey = br.readLine();
			ret.secretKey = br.readLine();
			ret.endpointUrl = br.readLine();
			ret.region = br.readLine();
			br.close();
		}
		catch (IOException e) { Errors.error(e); }
		
		return ret;
	}
	public AmazonS3Client connect(File credentials)
	{
		AmazonS3Client s3 = null;
		CredentialsStats cs = parseCredentials(credentials);

		BasicAWSCredentials c = new BasicAWSCredentials(cs.accessKey, cs.secretKey);

		ClientConfiguration cc = new ClientConfiguration();
		cc.setProtocol(Protocol.HTTPS); 
		//cc.setProtocol(Protocol.HTTP);
		cc.setMaxConnections(128);
		cc.setConnectionTimeout(36000000);
		cc.setSocketTimeout(36000000);
		//final int hintMB = 8 * 1024 * 1024;
		//cc.setSocketBufferSizeHints(hintMB, hintMB);
		
		s3 = new AmazonS3Client(c, cc);
		s3.setEndpoint(cs.endpointUrl);
		
		return s3;
	}
	
	public AmazonS3Client initialize(File credentials, String bucketName, boolean resetBucket) 
	{
		CredentialsStats cs = parseCredentials(credentials);
		AmazonS3Client s3 = connect(credentials);
		
		if(s3 != null)
		{
			try
			{				
				boolean found = false;
				for (Bucket bucket : s3.listBuckets()) 
				{ 
					if(bucket.getName().equals(bucketName)) { found = true; }
				}
				
				if(found == false)
				{
		            Log.getInstance().append("[Amazon S3] Creating bucket " + bucketName, Log.TRACE);
		            s3.createBucket(bucketName, cs.region);
				}
				else if(resetBucket == true) { emptyBucket(s3, bucketName); }
			}
			catch (AmazonServiceException ase) { processASE(ase); } 
			catch (AmazonClientException ace) { processACE(ace); }
		}
		
		return s3;
	}
	
	public long bucketByteSize(AmazonS3Client s3, String bucketKey) 
	{
		long ret = 0;
		try
		{
			ObjectListing ol = s3.listObjects(bucketKey);
			List<S3ObjectSummary> summaries = ol.getObjectSummaries();
			for(S3ObjectSummary os : summaries) { ret += os.getSize(); }
	
			while (ol.isTruncated() == true) 
			{
				ol = s3.listNextBatchOfObjects (ol);
				summaries = ol.getObjectSummaries();
				
				for(S3ObjectSummary os : summaries) { ret += os.getSize(); }
			}
			
		}
		catch (AmazonServiceException ase) { processASE(ase); } 
		catch (AmazonClientException ace) { processACE(ace); }
			
		return ret;
	}
	
	public void cloneBucket(AmazonS3Client s3, String bucketKey, String destBucketName) 
	{
		try
		{
			s3.createBucket(destBucketName);
			
			ObjectListing ol = s3.listObjects(bucketKey);
			List<S3ObjectSummary> summaries = ol.getObjectSummaries();
			
			for(S3ObjectSummary os : summaries)
			{
				s3.copyObject(os.getBucketName(), os.getKey(), destBucketName, os.getKey());
			}
	
			while (ol.isTruncated() == true) 
			{
				ol = s3.listNextBatchOfObjects (ol);
				summaries = ol.getObjectSummaries();
				
				for(S3ObjectSummary os : summaries)
				{
					s3.copyObject(os.getBucketName(), os.getKey(), destBucketName, os.getKey());
				}
			}
		}
		catch (AmazonServiceException ase) { processASE(ase); } 
		catch (AmazonClientException ace) { processACE(ace); }
	}
	
	public void emptyBucket(AmazonS3Client s3, String bucketName)
	{
		emptyBucket(s3, bucketName, false);
	}
	
	public void emptyBucket(AmazonS3Client s3, String bucketName, boolean ignoreErrors) 
	{
		try
		{			
			ObjectListing ol = s3.listObjects(bucketName);
			List<S3ObjectSummary> summaries = ol.getObjectSummaries();
			List<KeyVersion> keys = new ArrayList<KeyVersion>();
			
			for(S3ObjectSummary os : summaries) { keys.add(new KeyVersion(os.getKey())); }
			
			DeleteObjectsRequest multiDeleteReq = new DeleteObjectsRequest(bucketName);
			multiDeleteReq.setKeys(keys);
			
			if(keys.isEmpty() == false) { s3.deleteObjects(multiDeleteReq); }
	
			while (ol.isTruncated() == true) 
			{
				ol = s3.listNextBatchOfObjects (ol);
				summaries = ol.getObjectSummaries();
				
				keys.clear();
				for(S3ObjectSummary os : summaries) { keys.add(new KeyVersion(os.getKey())); }
				
				multiDeleteReq = new DeleteObjectsRequest(bucketName);
				multiDeleteReq.setKeys(keys);
				
				if(keys.isEmpty() == false) { s3.deleteObjects(multiDeleteReq); }
			}
		}
		catch (AmazonServiceException ase) { if(ignoreErrors == false) { processASE(ase); } } 
		catch (AmazonClientException ace) { if(ignoreErrors == false) { processACE(ace); } }
	}
}
