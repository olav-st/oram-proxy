package eoram.cloudexp.service.runners;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.artifacts.SystemParameters;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.interfaces.CompletionCallback;
import eoram.cloudexp.interfaces.ExternalClientInterface;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.service.application.RequestSource;
import eoram.cloudexp.utils.Errors;

/**
 * Represents an abstract runner.
 * A runner is a class that runs (i.e., plays, or replays) requests.
 *
 */
public abstract class AbstractRunner 
{
	protected Log log = Log.getInstance();
	protected SystemParameters sysParams = SystemParameters.getInstance();
	protected ClientParameters clientParams = ClientParameters.getInstance();
	
	protected SessionState ss = SessionState.getInstance();
	
	protected CryptoProvider cp = CryptoProvider.getInstance();

	protected ExternalClientInterface client = null;
	protected ExternalStorageInterface storage = null;
	
	public AbstractRunner(ExternalClientInterface c, ExternalStorageInterface s)
	{
		client = c; storage = s;
	}
	
	public void open(File stateFile, RequestSource inputRS)
	{
		client.open(storage, stateFile, ss.shouldReset());
	}
	
	public int run(RequestSource rs, int length)
	{
		int processed = 0;
		
		while(rs.hasNext() == true && processed < length)
		{
			Request req = rs.next();

			List<CompletionCallback> callbacks = new ArrayList<CompletionCallback>();
			callbacks.add(onNew(req));
			if(rs instanceof CompletionCallback)
			{
				callbacks.add((CompletionCallback)rs);
			}
			
			ScheduledRequest sreq = client.schedule(req, callbacks);
			
			boolean success = onScheduled(sreq);
			if(success == true)
			{
				processed++;
				ss.nextReqId++;
			}
			else
			{
				processFailure(sreq);
			}
		}
		
		return processed;
	}
	
	public abstract boolean onScheduled(ScheduledRequest sreq);

	public abstract CompletionCallback onNew(Request req);// { return null; }

	public void processFailure(ScheduledRequest sreq)
	{
		Errors.error("[AR] Failed on post-process of " + sreq.getRequest().getStringDesc() + " !");
	}
	
	public void close(String cloneStorageTo)
	{
		client.close(cloneStorageTo);
	}
}
