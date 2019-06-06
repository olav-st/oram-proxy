package eoram.cloudexp.implementation;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.artifacts.SystemParameters;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.WrappedListDataItem;
import eoram.cloudexp.interfaces.InternalStorageInterface;
import eoram.cloudexp.service.*;
import eoram.cloudexp.utils.AmazonS3Utils;
import eoram.cloudexp.utils.Errors;
import eoram.cloudexp.utils.MiscUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a (synchronous) storage interface for Amazon S3.
 * 
 *  @see AmazonS3AsyncStorage
 */
public class AmazonS3Storage implements InternalStorageInterface 
{
	private SystemParameters sysParams = SystemParameters.getInstance();
	private AmazonS3Utils utils = AmazonS3Utils.getInstance();
	
	private AmazonS3Client s3 = null;
	
	private String bucketName = null;
	
	private boolean resetBucket = false;
	
	public AmazonS3Storage(boolean shouldReset) { resetBucket = shouldReset; }

	@Override
	public void connect() 
	{
		bucketName = SessionState.getInstance().storageKey.toLowerCase();
		s3 = utils.initialize(sysParams.credentials, bucketName, resetBucket);
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op) 
	{
		String key = op.getKey();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				GetObjectRequest getReq = new GetObjectRequest(bucketName, key);

				if(op.getNumBytes() != -1)
				{
					getReq = getReq.withRange(op.getOffset(), op.getOffset() + op.getNumBytes());
				}

				S3Object o = s3.getObject(getReq);
				InputStream is = o.getObjectContent();

				SimpleDataItem sdi = new SimpleDataItem(MiscUtils.getInstance().ByteArrayFromInputStream(is));

				sop.onSuccess(sdi);
				try { is.close(); } catch (IOException e) { Errors.error(e); }
				
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadOperation op) 
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				ObjectMetadata metadata = new ObjectMetadata();
			    metadata.setContentLength(data.length);
			    
				PutObjectResult res = s3.putObject(bucketName, key, new ByteArrayInputStream(data), metadata);
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op) 
	{
		String key = op.getKey();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				s3.deleteObject(bucketName, key);
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation copyObject(CopyOperation op) 
	{
		String srcKey = op.getSourceKey();
		String destKey = op.getDestinationKey();
		
		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;
		
		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);
			
			try
			{
				s3.copyObject(bucketName, srcKey, bucketName, destKey);
				
				sop.onSuccess(new EmptyDataItem());
				return sop;
			}
			catch (AmazonServiceException ase) { if(lastAttempt == true) { utils.processASE(ase); } } 
			catch (AmazonClientException ace) { if(lastAttempt == true) { utils.processACE(ace); } }
		}
		
		sop.onFailure();
		return sop;
	}
	
	@Override
	public ScheduledOperation listObjects(ListOperation op) 
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		List<String> ret = new ArrayList<String>();
		try
		{
			ObjectListing ol = s3.listObjects(bucketName);
			List<S3ObjectSummary> summaries = ol.getObjectSummaries();
			for(S3ObjectSummary os : summaries) { ret.add(os.getKey()); }
	
			while (ol.isTruncated() == true) 
			{
				ol = s3.listNextBatchOfObjects (ol);
				summaries = ol.getObjectSummaries();
				
				for(S3ObjectSummary os : summaries) { ret.add(os.getKey()); }
			}
			
			sop.onSuccess(new WrappedListDataItem(ret));
		}
		catch (AmazonServiceException ase) { AmazonS3Utils.getInstance().processASE(ase); } 
		catch (AmazonClientException ace) { AmazonS3Utils.getInstance().processACE(ace); }
			
		return sop;
	}

	@Override
	public void disconnect() { s3.shutdown(); }
	
	@Override
	public long totalByteSize()
	{
		return utils.bucketByteSize(s3, bucketName);
	}

	@Override
	public void cloneTo(String to) 
	{
		utils.cloneBucket(s3, bucketName, to);
	}
}
