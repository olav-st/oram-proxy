package eoram.cloudexp.interfaces;

import eoram.cloudexp.service.*;

/**
 * Defines the internal interface of a storage system.
 */
public interface InternalStorageInterface 
{
	public void connect();
	
	public ScheduledOperation downloadObject(DownloadOperation op);
	public ScheduledOperation uploadObject(UploadOperation op);
	public ScheduledOperation deleteObject(DeleteOperation op);
	public ScheduledOperation copyObject(CopyOperation op);
	
	public ScheduledOperation listObjects(ListOperation op);
	
	public void disconnect();
	
	public long totalByteSize();
	
	public void cloneTo(String to);
}
