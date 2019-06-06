package eoram.cloudexp.service.runners;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.interfaces.CompletionCallback;
import eoram.cloudexp.interfaces.ExternalClientInterface;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.utils.Errors;

/**
 * Runs requests in parallel.
 */
public class ParallelRunner extends AbstractRunner
{
	protected boolean safe = false;
	protected ConcurrentHashMap<String, ScheduledRequest> pendingKeys = new ConcurrentHashMap<>();
	
	public ParallelRunner(ExternalClientInterface c, ExternalStorageInterface s, boolean safe)
	{
		super(c, s); this.safe = safe;
		
		if(safe == false && c.isSynchronous() == false)
		{ // this could be absolutely fine but issue a warning anyways.
			Errors.warn("[PR] Client " + c.getName() + " is asynchronous, and safety is off!");
		}
	}
	
	public ParallelRunner(ExternalClientInterface c, ExternalStorageInterface s)  { this(c, s, true); }

	@Override
	public synchronized CompletionCallback onNew(Request req) 
	{
		if(safe == true)
		{
			ScheduledRequest pendingSReq = pendingKeys.remove(req.getKey());
			if(pendingSReq != null && pendingSReq.isReady() == false)	
			{
				log.append("[PR] Stalling execution until the completion of req " + pendingSReq.getRequest().getId(), Log.INFO);
				pendingSReq.waitUntilReady();
				log.append("[PR] Wait over, req " + pendingSReq.getRequest().getId() + " completed", Log.INFO);
			}
		}
		return null;
	}
	
	@Override
	public synchronized boolean onScheduled(ScheduledRequest sreq) 
	{
		if(safe == true)
		{
			String key = sreq.getRequest().getKey();
			ScheduledRequest sreq2 = pendingKeys.get(key);
			Errors.verify(sreq2 == null);
			
			if(sreq.isReady() == false) { pendingKeys.put(key, sreq); }
		}
		return true; 
	}
}
