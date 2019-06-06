package eoram.cloudexp.interfaces;

import java.io.File;
import java.util.Collection;
import java.util.List;

import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledRequest;

/**
 * Defines the external interface of a client.
 */
public interface ExternalClientInterface 
{
	public void open(ExternalStorageInterface storage, File stateFile, boolean reset);
	
	public boolean isSynchronous();
	public String getName();
	
	public ScheduledRequest schedule(Request req, List<CompletionCallback> callbacks);
	
	public void waitForCompletion(Collection<ScheduledRequest> reqs);
	
	public List<ScheduledRequest> getPendingRequests();
	
	public void close(String cloneStorageTo);
}
