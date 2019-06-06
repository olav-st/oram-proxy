package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.evaluation.PerformanceEvaluationLogger;
import eoram.cloudexp.interfaces.CompletionCallback;
import eoram.cloudexp.interfaces.ExternalClientInterface;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.interfaces.InternalClientInterface;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.utils.Errors;

import java.io.File;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Implements an adapter between the external client interface and the internal client's logic.
 * <p><p>
 * The adapter takes care of handling performance events and completion callbacks so that the (ORAM) clients do not need to.
 */
public class ClientAdapter implements ExternalClientInterface 
{
	protected Log log = Log.getInstance();
	protected PerformanceEvaluationLogger pe = PerformanceEvaluationLogger.getInstance();
	
	protected InternalClientInterface client = null;
	protected CompletionThread completionThread = null;
	
	protected boolean opened = false;
	
	public class CompletionThread extends Thread
	{
		protected volatile boolean done = false;		
		protected BlockingQueue<Map.Entry<ScheduledRequest, List<CompletionCallback>>> queue = null;
		
		public CompletionThread(BlockingQueue<Map.Entry<ScheduledRequest, List<CompletionCallback>>> q) { queue = q; }
		
		public void run()
		{
			Set<Map.Entry<ScheduledRequest, List<CompletionCallback>>> pending
						= new HashSet<Map.Entry<ScheduledRequest, List<CompletionCallback>>>();
			
			while(done == false || queue.size() > 0 || pending.size() > 0)
			{
				// drain stuff to pending
				queue.drainTo(pending);
				
				if(pending.size() == 0) 
				{ 
					// poll 
					try 
					{
						Map.Entry<ScheduledRequest, List<CompletionCallback>> entry = queue.poll(5, TimeUnit.MILLISECONDS);
						if(entry != null) { pending.add(entry); }
					} 
					catch (InterruptedException e1) {	e1.printStackTrace(); }
					continue; 
				}
				Iterator<Map.Entry<ScheduledRequest, List<CompletionCallback>> > iter = pending.iterator();
				while(iter.hasNext() == true)
				{
					Map.Entry<ScheduledRequest, List<CompletionCallback>> entry = iter.next();
					ScheduledRequest sreq = entry.getKey();
					List<CompletionCallback> callbacks = entry.getValue();
					if(sreq.isReady() == true)
					{						
						iter.remove(); // remove
						
						complete(sreq, callbacks); // complete the operation
					}
				}
			}
		}
		
		public void shutdown()
		{
			done = true;
		}
	}
	
	protected BlockingQueue<Map.Entry<ScheduledRequest, List<CompletionCallback>>> scheduledQueue
			= new LinkedBlockingQueue<Map.Entry<ScheduledRequest, List<CompletionCallback>>>();
	
	protected Set<ScheduledRequest> pendingSet = new HashSet<ScheduledRequest>();
	
	private File stateFile = null;
	
	public ClientAdapter(InternalClientInterface c)
	{
		client = c; opened = false;
		completionThread = new CompletionThread(scheduledQueue);
	}
	
	protected void complete(ScheduledRequest sreq, List<CompletionCallback> callbacks)
	{
		assert(sreq.isReady() == true);
		
		if(callbacks != null)
		{
			boolean success = sreq.wasSuccessful();
			if(success == true)
			{
				for(CompletionCallback callback : callbacks)
				{
					if(callback != null)
					{
						callback.onSuccess(sreq);
					}
				}
			}
			else
			{
				for(CompletionCallback callback : callbacks)
				{
					if(callback != null)
					{
						callback.onFailure(sreq);
					}
				}
			}
		}
		
		pe.completeRequest(sreq); // -----------------
		
		log.append("[CA] Just completed " + sreq.getRequest().getStringDesc(), Log.INFO);
	}
	
	@Override
	public void open(ExternalStorageInterface storage, File stateFile, boolean reset) 
	{
		assert(opened == false);
		
		log.append("[CA] Opening client...", Log.INFO);
		
		completionThread.start(); // start the completion thread
		
		pe.openCall(); // --------
		
		client.open(storage, stateFile, reset);
		
		pe.openDone(); // --------
		
		opened = true;
		
		this.stateFile  = stateFile; // keep a pointer on the state file for later
		
		log.append("[CA] Client opened.", Log.INFO);
	}

	@Override
	public boolean isSynchronous() 
	{
		return client.isSynchronous();
	}

	@Override
	public ScheduledRequest schedule(Request req, List<CompletionCallback> callbacks)
	{
		assert(opened == true);
		
		log.append("[CA] Scheduling " + req.getStringDesc(), Log.INFO);
		
		ScheduledRequest scheduled = null;
		
		if(req.getType() == RequestType.PUT)
		{
			assert(req instanceof PutRequest);
			PutRequest put = (PutRequest)req;
			
			byte[] val = put.getValue().getData();
			
			ClientParameters clientParams = ClientParameters.getInstance();
			if(val.length != clientParams.contentByteSize && clientParams.noSplit == false)
			{
				if(val.length > clientParams.contentByteSize) { Errors.error("Invalid PUT request data"); }
				
				val = Arrays.copyOf(val, clientParams.contentByteSize); // ensure val has the correct size
				put.setValue(new SimpleDataItem(val));
			}
		}
		
		pe.scheduleRequest(req); // -----------------
		
		if(req.getType() == RequestType.GET)
		{
			assert(req instanceof GetRequest);
			scheduled = client.scheduleGet((GetRequest)req);
		}
		else
		{
			assert(req instanceof PutRequest);
			scheduled = client.schedulePut((PutRequest)req);
		}
		
		if(isSynchronous() == true) // call the callback immediately
		{
			complete(scheduled, callbacks);
		}
		else
		{ 	// add the request to the queue
			try { scheduledQueue.put(new AbstractMap.SimpleEntry<ScheduledRequest, List<CompletionCallback>>(scheduled, callbacks)); }
			catch (InterruptedException e) { e.printStackTrace(); }
			
			pendingSet.add(scheduled); // also add it to the set
			
			Pollable.removeCompleted(pendingSet); // so the set doesn't get too large
		}
		
		return scheduled;
	}

	@Override
	public synchronized void waitForCompletion(Collection<ScheduledRequest> reqs) 
	{
		assert(opened == true);
		assert(completionThread.isAlive() == true);
		
		Pollable.waitForCompletion(reqs);
	}

	@Override
	public synchronized List<ScheduledRequest> getPendingRequests() 
	{
		assert(opened == true);
		
		Pollable.removeCompleted(pendingSet); // so the set doesn't get too large
		
		List<ScheduledRequest> ret = new ArrayList<ScheduledRequest>();
		ret.addAll(pendingSet);
		
		Collections.sort(ret);
		return ret;
	}

	@Override
	public void close(String cloneStorageTo) 
	{
		assert(opened == true);
		
		log.append("[CA] Closing client...", Log.INFO);
		
		long peakBytes = client.peakByteSize();
		pe.setPeakByteSize(peakBytes);
		
		pe.closeCall(); // --------
		
		waitForCompletion(getPendingRequests());
		
		completionThread.shutdown();
		try { completionThread.join(); } catch (InterruptedException e) { e.printStackTrace();	}
		
		if(cloneStorageTo != null) { log.append("[CA] Cloning storage to: " + cloneStorageTo, Log.INFO); }
		client.close(cloneStorageTo);
		
		pe.closeDone(); // --------
		
		opened = false;
		
		// now that the client is closed, let's the get local byte size
		long bytes = stateFile.length();
		pe.setLocalByteSize(bytes);
		
		log.append("[CA] Client closed...", Log.INFO);		
	}

	@Override
	public String getName() { return client.getName(); }
}
