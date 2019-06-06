package eoram.cloudexp.threading;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.utils.Errors;

/**
 * Represents an abstract jobs queue.
 */
public abstract class ThreadedJobsQueue 
{
	protected Log log = Log.getInstance();
	
	public enum SchedulingPriority 
	{
		HIGH, NORMAL;
	}
	
	public class JobItem 
	{
		protected String key = null;
		protected Job<?> job = null;
		protected SchedulingPriority priority = null;
		
		protected JobItem next = null;
		protected boolean completeWhenDone;
		
		public JobItem(String k, Job<?> j, SchedulingPriority prio, boolean cwd)	
		{ 
			key = k; job = j; priority = prio; next = null;
			completeWhenDone = cwd;
		}
		
		public String toString()
		{
			String ret = "JobItem(" + key + ", " + job.toString() + ", " + priority.toString() + ", next ";
			ret += ((next == null) ? "= null" : "exists") + ", cwd: " + completeWhenDone + ")";
			
			return ret;
		}
	}
	
	protected class SchedulingJobsQueue
	{
		protected LinkedHashMap<String, JobItem> highPriorityMap = new LinkedHashMap<String, JobItem>();
		protected LinkedHashMap<String, JobItem> normalPriorityMap = new LinkedHashMap<String, JobItem>();
		
		protected Map<String, JobItem> pendingItems = new HashMap<String, JobItem>();
		
		private JobItem get(LinkedHashMap<String, JobItem> map)
		{
			Iterator<JobItem> iter = map.values().iterator();
			if(iter.hasNext() == true)
			{
				JobItem item = iter.next();
				iter.remove();
				
				Errors.verify(pendingItems.containsKey(item.key) == false);
				pendingItems.put(item.key, item);
				
				return item;
			}
			return null;
		}
		
		protected synchronized JobItem poll()
		{
			JobItem item = null;
			
			item = get(highPriorityMap);
			if(item == null) { item = get(normalPriorityMap); }
			
			return item;
		}
		
		protected synchronized void offer(JobItem item)
		{
			Errors.verify(item != null);
			
			LinkedHashMap<String, JobItem> map = item.priority == SchedulingPriority.HIGH ? highPriorityMap : normalPriorityMap;
			if(map.containsKey(item.key) == true)
			{
				// then enqueue it as next of the item
				JobItem prevItem = map.get(item.key);
				while(prevItem.next != null) { prevItem = prevItem.next; }
				prevItem.next = item;
			}
			else
			{
				if(pendingItems.containsKey(item.key) == true)
				{
					// then make it the next of the item
					JobItem prevItem = pendingItems.get(item.key);
					while(prevItem.next != null) { prevItem = prevItem.next; }
					prevItem.next = item;
				}
				else
				{
					// enqueue it normally
					map.put(item.key, item);
				}
			}
		}

		public synchronized boolean isEmpty() 
		{
			return ((highPriorityMap.isEmpty() == true) && (normalPriorityMap.isEmpty() == true));
		}
		
		public synchronized void complete(String key)
		{
			if(pendingItems.containsKey(key) == false)
			{
				//{ log.append("[TJQ] complete called on an already completed job (key: " + key + ")", Log.TRACE); }
				return;
			}
			
			JobItem item = pendingItems.remove(key);
			//{ log.append("[TJQ] complete called on job with key " + key + ", has a next job: " + (item.next != null), Log.TRACE); }
			
			if(item.next != null) 
			{
				//{ log.append("[TJQ] job: " + item + " has a next job", Log.TRACE); }
				JobItem next = item.next;
				item.next = null;
				offer(next);
			}
			
			//{ log.append("[TJQ] pendingItems.size(): " + pendingItems.size() + ", highPriorityMap.size(): " + highPriorityMap.size() + ", normalPriorityMap.size(): " + normalPriorityMap.size(), Log.TRACE); }
		}
		
		public synchronized int jobsWithKey(String key, boolean pending)
		{
			JobItem item = null;
			if(pending == true) 
			{
				item = pendingItems.get(key);
			}
			else 
			{
				item = highPriorityMap.get(key);
				if(item == null)
				{
					item = normalPriorityMap.get(key);
				}
			}
			
			if(item == null) { return 0; }
			
			int ret = 1;
			while(item.next != null) { item = item.next; ret++; }
			return ret;
		}
	}
	
	protected class Worker extends Thread
	{
		protected String name = null;
		protected AtomicBoolean done = null;
		
		protected Worker(String n, AtomicBoolean d) { name = n; done = d; }
		
		protected final int sleepTimeMillis = 10;
		
		@Override
		public void run() 
		{
			while(done.get() == false || queue.isEmpty() == false)
			{
				JobItem item = queue.poll();

				if(item != null) { process(item); }
				else
				{
					try { Thread.sleep(sleepTimeMillis); } catch (InterruptedException e) { }
				}
			}
		}

		private void process(JobItem item) 
		{
			//{ log.append("[TJQ] Processing job: " + item, Log.TRACE); }
			
			item.job.call();
			
			//{ log.append("[TJQ] Job: " + item + " has been processed.", Log.TRACE); }
			
			if(SessionState.getInstance().debug == true) // debug only
			{
				if(item.next != null) 
				{ 
					Errors.warn("[TJQ] job: " + item + " has a next job (" + item.next + ") which was not re-enqueued (complete was not called)");
				}
			}
		
			if(item.completeWhenDone == true)
			{
				queue.complete(item.key); // complete the item
			}
		}
	}
	
	protected List<Worker> workers = new ArrayList<Worker>();
	protected AtomicBoolean done = new AtomicBoolean(false);
	
	protected SchedulingJobsQueue queue = new SchedulingJobsQueue();
	protected Random rng = new SecureRandom(); 
	
	protected ThreadedJobsQueue(int threads)
	{
		for(int i=0; i<threads; i++)
		{
			Worker worker = new Worker("worker-" + i, done);
			workers.add(worker);
			worker.start();
		}
		
		log.append("[TJQ] Create threaded jobs queue of " + threads + " threads.", Log.INFO);
	}
	
	private void enqueue(JobItem item) 
	{ 
		//{ log.append("[TJQ] Enqueuing job: " + item, Log.TRACE); }
		queue.offer(item); 
	}
	
	private String getRandomKey() 
	{
		CryptoProvider cp = CryptoProvider.getInstance();
		return "j-" + cp.truncatedHexHashed(rng.nextLong() + "", 6).toLowerCase();
	}
	
	public synchronized void scheduleJob(Job<?> job, SchedulingPriority prio)
	{
		scheduleJob(null, job, prio);
	}
	
	public synchronized void scheduleJob(String key, Job<?> job, SchedulingPriority prio)
	{
		boolean completeWhenDone = false;
		if(key == null) { key = getRandomKey(); completeWhenDone = true; }
		JobItem item = new JobItem(key, job, prio, completeWhenDone);
		enqueue(item);
	}
	
	public void shutdown() // don't synchronize this, it may lead to a deadlock
	{
		done.set(true); // ask for shutdown
		log.append("[TJQ] Waiting for all tasks to terminate...", Log.INFO);
		
		try { Thread.sleep(15); } catch (InterruptedException e) { Errors.error(e); }
		
		for(int i=0; i<workers.size(); i++)
		{
			try { workers.get(i).join();} catch (InterruptedException e) { }
		}
		
		workers.clear();
	}

	public synchronized void complete(String key) 
	{
		queue.complete(key);		
	}

	public synchronized int jobsWithKey(String key, boolean pending) 
	{
		return queue.jobsWithKey(key, pending);
	}
}
