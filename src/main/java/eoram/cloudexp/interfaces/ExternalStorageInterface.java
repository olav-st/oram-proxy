package eoram.cloudexp.interfaces;

import eoram.cloudexp.service.*;

/**
 * Defines the external interface of a storage system.
 */
public interface ExternalStorageInterface 
{
	public void connect();
	
	public ScheduledOperation downloadObject(DownloadOperation op);
	public ScheduledOperation uploadObject(UploadOperation op);
	public ScheduledOperation deleteObject(DeleteOperation op);
	public ScheduledOperation copyObject(CopyOperation op);
	
	public ScheduledOperation listObjects(ListOperation op);

	public void disconnect();
	
	public void cloneTo(String to);
}
