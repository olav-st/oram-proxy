package eoram.cloudexp.schemes;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.data.*;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.data.encoding.ObliviStoreHeader;
import eoram.cloudexp.implementation.AbstractClient;
import eoram.cloudexp.pollables.Completable;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.primitives.*;
import eoram.cloudexp.schemes.primitives.Partition.Level;
import eoram.cloudexp.service.DeleteOperation;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.threading.Job;
import eoram.cloudexp.threading.SingleThreadJobsQueue;
import eoram.cloudexp.threading.ThreadedJobsQueue;
import eoram.cloudexp.threading.ThreadedJobsQueue.SchedulingPriority;
import eoram.cloudexp.utils.Errors;

/**
 * Implements the ObliviStore scheme (see Stefanov, Emil, and Elaine Shi. "Oblivistore: High performance oblivious cloud storage." IEEE S&P, 2013.),
 * <p><p>
 * This implementation closely follows the C# implementation obtained from the authors.
 */
public class ObliviStoreClient extends AbstractClient
{
	public static void saveBlocksMap(ObjectOutputStream os, Map<Integer, BlockDataItem> map) throws Exception
	{
		os.writeInt(map.size());
		for(int blockId : map.keySet())
		{
			os.writeInt(blockId);
			BlockDataItem bdi = map.get(blockId);
			Errors.verify(bdi != null);
			bdi.save(os);
		}
	}
	public static void loadBlocksMap(ObjectInputStream is, Map<Integer, BlockDataItem> map) throws Exception 
	{
		int size = is.readInt();
		while(size > 0)
		{
			int blockId = is.readInt();
			BlockDataItem bdi = new BlockDataItem(is);
			map.put(blockId, bdi);
			size--;
		}
	}
	
	protected class Position
	{
		public static final int InvalidPartitionIdx = -1;
		public static final int InvalidLevelIdx = -1;
		public static final int InvalidOffsetInLevel = -1;
		public static final int ZeroBlock = -2;
		
		protected int partitionIdx = InvalidPartitionIdx;
		protected int levelIdx = InvalidLevelIdx; 
		protected int offsetInLevel = InvalidOffsetInLevel;
		
		protected Position(int idx) { this(idx, InvalidLevelIdx); }
		protected Position(int idx, int level) { this(idx, level, InvalidOffsetInLevel); }
		protected Position(int idx, int level, int offset) { partitionIdx = idx; levelIdx = level; offsetInLevel = offset; }
	
		public String toString() { return "(" + partitionIdx + ", " + levelIdx + ", " + offsetInLevel + ")"; }
		
		public void save(ObjectOutputStream os) throws Exception
		{
			os.writeInt(partitionIdx);
			os.writeInt(levelIdx);
			os.writeInt(offsetInLevel);
		}
		
		public Position(ObjectInputStream is) throws Exception
		{
			partitionIdx = is.readInt();
			levelIdx = is.readInt();
			offsetInLevel = is.readInt();
		}
	}
	
	protected class PositionMap
	{
		/** Implementation with Map
		protected Map<Integer, Position> map = new HashMap<Integer, Position>();
		public Position getPos(int blockId) { return map.get(blockId); }
		public void setPos(int blockId, Position newPos) { map.put(blockId, newPos); }
		**/
		
		/** Implementation with Array **/
		protected Position[] array = null;
		protected PositionMap(int s) { array = new Position[s]; }
		public Position getPos(int blockId) { return array[blockId]; }
		public void setPos(int blockId, Position newPos) { array[blockId] = newPos; }
		
		public void save(ObjectOutputStream os) throws Exception  
		{ 
			os.writeInt(array.length);
			for(int i=0; i<array.length; i++)
			{
				Position pos = array[i];
				pos.save(os);
			}
		}
		
		protected PositionMap(ObjectInputStream is) throws Exception 
		{ 
			int length = is.readInt();
			array = new Position[length];
			for(int i=0; i<array.length; i++)
			{
				array[i] = new Position(is);
			}
		}
	}
	
	protected class CacheSlot
	{
		protected Map<Integer, BlockDataItem> blocksMap = new HashMap<Integer, BlockDataItem>();

		protected CacheSlot() {}
		
		public BlockDataItem dequeue(int key) { return blocksMap.remove(key); }

		public void add(BlockDataItem bdi) 
		{ 
			ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader();
			int blockId = header.getBlockId();
			blocksMap.put(blockId, bdi); 
		}

		public Collection<? extends BlockDataItem> removeAndPad(int jobSize) 
		{
			Errors.verify(jobSize > 0);
			
			List<Integer> blockIDs = new ArrayList<Integer>();
			blockIDs.addAll(blocksMap.keySet());
			Collections.shuffle(blockIDs, rng);
			
			List<BlockDataItem> ret = new ArrayList<BlockDataItem>();
			for(int blockId : blockIDs)
			{
				ret.add(blocksMap.remove(blockId));
				if(ret.size() == jobSize) { break; }
			}
			
			evictionCacheNumItems -= ret.size();
			
			while(ret.size() < jobSize) { ret.add(getDummyBlock()); }
			
			return ret;
		}
		
		public void save(ObjectOutputStream os)  throws Exception { saveBlocksMap(os, blocksMap); }
		protected CacheSlot(ObjectInputStream is) throws Exception { loadBlocksMap(is, blocksMap); }
	}
	
	public enum ShufflingStep { Setup, Reading, LocalShuffle, Writing, Finalize };
	
	protected class ShuffleJob
	{
		protected List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
		protected long reqId = 0;
		protected ShufflingStep step = null;
		protected Partition partition = null;
		
		protected int workToDo = 0;
		protected int storageWorkToDo = 0;
		
		protected int jobSize = 0;
		protected List<Partition.Level> levelsToShuffle = null;
		protected List<Partition.Level> shuffleToLevels = null;
		protected int sizeOfShuffleToLevels = 0;
		
		protected List<ScheduledOperation> pendingCacheIns = null;
		
		protected ShuffleJob next = null;
		
		
		public ShuffleJob(long r, Partition p) 
		{ reqId = r; partition = p; step = ShufflingStep.Setup; next = null; }
	}
	
	protected class Shuffler// extends Thread
	{
		private final String shufflerJobKey = "shuffle-worker";
		protected ThreadedJobsQueue q = jobsQueue; 
		///// protected ThreadedJobsQueue q = new SingleThreadJobsQueue(); 
		
		protected ShuffleJob[] activeJobs = null;
		protected Map<ShufflingStep, BlockingQueue<ShuffleJob>> queue = null;
		
		protected AtomicBoolean done = new AtomicBoolean(false);
		
		protected final int maxUnassignedWork = 1 << 16;
		protected AtomicInteger unassignedWork = new AtomicInteger(0);
		
		protected final int minShufflingBlockCount = 1 << 10; // just as in obliviStore
		protected AtomicInteger unassignedStorageWork = new AtomicInteger(minShufflingBlockCount);
		
		// called once for each request
		public void assignWork() { unassignedWork.addAndGet(shufflingWorkPerRequest); }
		
		public void assignWork(int work)  { unassignedWork.addAndGet(work); }
		
		public int unassignedWork() { return unassignedWork.get(); }
		
		public Shuffler() 
		{
			queue = new HashMap<ShufflingStep, BlockingQueue<ShuffleJob>>();
			queue.put(ShufflingStep.Setup, new LinkedBlockingQueue<ShuffleJob>());
			queue.put(ShufflingStep.Reading, new LinkedBlockingQueue<ShuffleJob>());
			queue.put(ShufflingStep.LocalShuffle, new LinkedBlockingQueue<ShuffleJob>());
			queue.put(ShufflingStep.Writing, new LinkedBlockingQueue<ShuffleJob>());
			queue.put(ShufflingStep.Finalize, new LinkedBlockingQueue<ShuffleJob>());
			
			activeJobs = new ShuffleJob[partitions.size()];
		}
		
		public void shutdown() 
		{ 
			// make sure all active jobs have terminated before we shutdown
			while(getActiveJobsCount() > 0)
			{
				try { Thread.sleep(15); } catch(InterruptedException e) { Errors.error(e); } 
				
				if(getActiveJobsCount() > 0)
				{
					int scheduledButNotRunning = q.jobsWithKey(shufflerJobKey, false);
					int running = q.jobsWithKey(shufflerJobKey, true);
					if(running > 1) { scheduledButNotRunning += running-1; }
					if(scheduledButNotRunning == 0) { scheduleWorker(); }
				}
			}
			
			q.shutdown();
			
			done.set(true); 
		}
		
		private void doStep(ShufflingStep step, AtomicInteger work, AtomicInteger storageWork, AtomicBoolean someWorkDone)
		{
			BlockingQueue<ShuffleJob> jobs = queue.get(step);
			int itemsToProcess = jobs.size();
			while(itemsToProcess > 0 && work.get() > 0 && (storageWork.get() > 0 || step != ShufflingStep.Reading))
			{
				ShuffleJob job = null;
				
				try { job = jobs.take(); } catch (InterruptedException e) { Errors.error(e); }
				
				Errors.verify(job != null);
				itemsToProcess--;
				
				job.workToDo = work.getAndSet(0);
				job.storageWorkToDo = storageWork.getAndSet(0);
				
				Errors.verify(step == job.step);
				switch(job.step)
				{
				case Setup: doSetup(job, someWorkDone); break;
				case Reading: doReading(job, someWorkDone); break;
				case LocalShuffle: doLocalShuffle(job, someWorkDone); break;
				case Writing: doWriting(job, someWorkDone); break;
				case Finalize: doFinalize(job, someWorkDone); break;
				}
				
				work.addAndGet(job.workToDo > 0 ? job.workToDo : 0); job.workToDo = 0;
				storageWork.addAndGet(job.storageWorkToDo > 0 ? job.storageWorkToDo : 0); job.storageWorkToDo = 0;
			}
		}
		
		private int getActiveJobsCount()
		{
			int ret = 0;
			for(ShuffleJob j : activeJobs)
			{
				if(j != null) { ret++; }
			}
			return ret;
		}
		
		private void doLoop(AtomicBoolean someWorkDone)
		{
			someWorkDone.set(false);

			int w = unassignedWork.get(); // assign work
			AtomicInteger work = new AtomicInteger(w);
			
			int sw = unassignedStorageWork.get(); // assign storage work
			AtomicInteger storageWork = new AtomicInteger(sw);
			
			//{ log.append("[OSC (doWork)] doStep(Setup)----", Log.TRACE); }
			doStep(ShufflingStep.Setup, work, storageWork, someWorkDone);
			//{ log.append("[OSC (doWork)] doStep(Reading)----", Log.TRACE); }
			doStep(ShufflingStep.Reading, work, storageWork, someWorkDone);
			//{ log.append("[OSC (doWork)] doStep(LocalShuffle)----", Log.TRACE); }
			doStep(ShufflingStep.LocalShuffle, work, storageWork, someWorkDone);
			//{ log.append("[OSC (doWork)] doStep(Writing)----", Log.TRACE); }
			doStep(ShufflingStep.Writing, work, storageWork, someWorkDone);
			//{ log.append("[OSC (doWork)] doStep(Finalize)----", Log.TRACE); }
			doStep(ShufflingStep.Finalize, work, storageWork, someWorkDone);		
			
			int workDone = work.get() - w;
			unassignedWork.addAndGet(workDone);
			if(unassignedWork.get() > maxUnassignedWork) { unassignedWork.set(maxUnassignedWork); }
			
			int storageWorkDone = storageWork.get() - sw;
			unassignedStorageWork.addAndGet(storageWorkDone);
			if(unassignedStorageWork.get() > minShufflingBlockCount) { unassignedStorageWork.set(minShufflingBlockCount); }
		}
		
		private void doWork()
		{	
			if(getActiveJobsCount() == 0) 
			{
				q.complete(shufflerJobKey); // complete that job
				return; 
			}
			
			
			AtomicBoolean someWorkDone = new AtomicBoolean(true);		
			while(someWorkDone.get() == true)
			{
				doLoop(someWorkDone);
			}
			
			if(unassignedWork.get() <= 0 || unassignedStorageWork.get() <= 0) 
			{
				Errors.warn("[OSC (doWork)] We are starving (unassigned work: " + unassignedWork.get() + ", unassigned storage work: "+ unassignedStorageWork.get() +")!!");
				if(unassignedWork.get() <= 0) { unassignedWork.addAndGet(shufflingWorkPerRequest); }
				if(unassignedStorageWork.get() <= 0) { unassignedStorageWork.addAndGet(shufflingWorkPerRequest); }
			}
			
			if(done.get() == false && getActiveJobsCount() > 0) // must re-enqueue ourselves
			{
				int scheduledButNotRunning = q.jobsWithKey(shufflerJobKey, false);
				int running = q.jobsWithKey(shufflerJobKey, true);
				if(running > 1) { scheduledButNotRunning += running-1; }
				if(scheduledButNotRunning == 0) { scheduleWorker(); }
			}
			
			q.complete(shufflerJobKey); // complete that job
		}
		
		private void scheduleWorker()
		{
			// schedule a worker
			Callable<Void> c = new Callable<Void>()
			{
				@Override
				public Void call() throws Exception 
				{
					doWork();
					return null;
				}
			};
			Job<Void> j = new Job<Void>(shufflerJobKey + "-doWork", c);
			q.scheduleJob(shufflerJobKey, j, SchedulingPriority.NORMAL);
		}
		
		protected synchronized void addJob(long reqId, Partition p, CountDownLatch l)
		{
			ShuffleJob job = null;
			
			int partitionIdx = p.getIdx();
			ShuffleJob active = activeJobs[partitionIdx];
			
			if(active == null)
			{
				job = new ShuffleJob(reqId, p);
				activeJobs[partitionIdx] = job;
			}
			else
			{
				ShuffleJob prev = active;
				if(prev.next == null) 
				{
					job = new ShuffleJob(reqId, p);
					prev.next = job;
				}
				else 
				{
					while(prev.next != null && prev.next.jobSize >= maxPartitionSize/2)  //while(prev.next != null && prev.next.jobSize >= topLevelRealBlocksCount)
					{ prev = prev.next; }
					
					if(prev.next == null)
					{
						job = new ShuffleJob(reqId, p);
						prev.next = job;
					}
					job = prev.next; 
				}
			}
			active = activeJobs[partitionIdx];
			
			job.latches.add(l);
			job.jobSize++;

			if(job.jobSize == 1 && job == active)
			{
				BlockingQueue<ShuffleJob> setupQueue = queue.get(ShufflingStep.Setup);
				try { setupQueue.put(job); } catch (InterruptedException e) { Errors.error(e); }
				
			}
		
			int scheduledButNotRunning = q.jobsWithKey(shufflerJobKey, false);
			if(scheduledButNotRunning == 0 || (synchronous == true && scheduledButNotRunning <= 1)) { scheduleWorker(); }
			else
			{
				;
				//{ log.append("[OSC (addJob)] not scheduling new worker (scheduled: " + scheduledButNotRunning + ")", Log.TRACE); }
			}
		}
		private String levelsToString(List<Level> levels)
		{
			String ret = "["; int i = 0;
			for(Partition.Level level : levels) { if(i > 0) { ret += ", "; } ret += level.getIdx(); i++; } ret += "]";
			return ret;
		}

		private void doSetup(ShuffleJob job, AtomicBoolean someWorkDone)
		{
			Partition partition = job.partition;
			
			// 1. start shuffling
			if(partition.atomicSetShuffling() == false)
			{ 
				// wait for shuffling (some other thread is currently doing the job)
				internalEnqueueJob(job);
				return;
			} 

			// take snapshot of partition job size
			Errors.verify(job.jobSize > 0);
			
			someWorkDone.set(true); // we did something

			job.levelsToShuffle = new ArrayList<Level>();
			job.shuffleToLevels = new ArrayList<Level>();
			
			partition.getLevelsForReshuffling(job.jobSize, job.levelsToShuffle, job.shuffleToLevels);
			
			// -d-
			if(ss.debug == true)
			{
				if(log.shouldLog(Log.TRACE) == true)
				{
					String sl = levelsToString(job.levelsToShuffle);
					String dl = levelsToString(job.shuffleToLevels);
					{ log.append("[OSC (doSetup)] Starting job on partition " + partition.getIdx() + ", jobSize: " + job.jobSize + ", reshuffling: " + sl + " -> " + dl, Log.TRACE); }
				}
			}
		
			for(Level level : job.levelsToShuffle) { level.markForShuffling(); } // mark levels for shuffling
			
			job.sizeOfShuffleToLevels = 0;
			for(Level shuffleTo : job.shuffleToLevels) { job.sizeOfShuffleToLevels += shuffleTo.getSize(); }		
			
			if(ss.debug == true)
			{
				if(log.shouldLog(Log.TRACE))
				{			
					String unreadBlocks = "["; int i = 0;
					for(Level level : partition.getLevels()) { if(i > 0) { unreadBlocks += ", "; } unreadBlocks += level.unreadBlocksForShuffling(); i++; } unreadBlocks += "]";
					{ log.append("[OSC (doSetup)] Job on partition " + partition.getIdx() + " will read the following number of blocks for shuffling: " + unreadBlocks, Log.TRACE); }
				}
			}
			
			job.step = ShufflingStep.Reading; // move to the reading step
			internalEnqueueJob(job);
		}
		
		private void doReading(final ShuffleJob job, AtomicBoolean someWorkDone)
		{
			if(job.workToDo <= 0) { internalEnqueueJob(job); return; } // no work to be done
			
			final Partition partition = job.partition;
			
			// 2. cache in and reserve space
			job.pendingCacheIns = new ArrayList<ScheduledOperation>();
			
			int workToDo = job.workToDo;
			
			boolean doneWithWork = false;
			while(job.workToDo > 0 && doneWithWork == false)
			{
				doneWithWork = true;
				
				someWorkDone.set(true); // we did something

				for(Partition.Level level : job.levelsToShuffle)
				{
					while(level.unreadBlocksForShuffling() > 0)
					{
						doneWithWork = false;
						
						// issue a cache-in request for the next unread block
						ScheduledOperation sop = level.readNextBlockTowardsShuffling(job.reqId);
						if(sop != null)
						{
							job.pendingCacheIns.add(sop);
						}
						
						job.workToDo--;
						if(job.workToDo == 0) { break; }
					}
					if(job.workToDo == 0) { break; }
				}
			}
			
			job.storageWorkToDo -= job.pendingCacheIns.size();
			
			final boolean allCachedIn = doneWithWork;
			
			// -d-
			//{ log.append("[OSC (doReading)] Job on partition " + partition.getIdx() + ", workToDo: " + workToDo + ", scheduledWork: " + (workToDo - job.workToDo), Log.TRACE); }
			
			Runnable r = new Runnable() 
			{
				@Override
				public void run() 
				{
					Pollable.waitForCompletion(job.pendingCacheIns); // wait for all cache-ins to be done
					
					// -d-
					//{ log.append("[OSC (doReading)] Job on partition " + partition.getIdx() + ", done waiting for pending cache-ins; allCachedIn: " + allCachedIn, Log.TRACE); }
					
					if(allCachedIn == true)
					{
						job.step = ShufflingStep.LocalShuffle; // move to the local shuffle step
					}
					internalEnqueueJob(job);
				}
			};
			submitExecutorTask(r);
		}
		
		private void doLocalShuffle(ShuffleJob job, AtomicBoolean someWorkDone)
		{
			Partition partition = job.partition;
			
			// 3. Upon completion of all cache-ins
			
			// get a write lock on the partition
			/*--------*/ partition.lock(true); /*--------*/
			
			
			someWorkDone.set(true); // we did something
			
			// Fetch jobSize blocks from eviction cache corresponding to this partition, pad with dummy block
			// 		increment eviction semaphore by 'jobSize'
			List<BlockDataItem> fromEvictionCache = getAndPadFromEvictionCache(partition.getIdx(), job.jobSize);
			evictionSemaphore.release(job.jobSize); Errors.verify(fromEvictionCache.size() == job.jobSize);
			
			Map<Integer, Map.Entry<Integer, Integer>> m = partition.localShuffle(rng, job.levelsToShuffle, job.shuffleToLevels, fromEvictionCache, earlyCacheInsSemaphore, job.sizeOfShuffleToLevels, getDummyBlock());
			
			// update the position map
			for(int blockId : m.keySet())
			{
				Errors.verify(blockId >= 0 && blockId < clientParams.maxBlocks);
				Entry<Integer, Integer> entry = m.get(blockId);
				int levelIdx = entry.getKey();
				int offset = entry.getValue();
				
				Position newPos = new Position(partition.getIdx(), levelIdx, offset);
				setBlockPos(blockId, newPos);
			}
			
			final boolean tryAvoidEventualConsistencyIssues = false;
			//final boolean tryAvoidEventualConsistencyIssues = true;
			
			if(tryAvoidEventualConsistencyIssues == true)
			{
				final List<String> keysToDelete = new ArrayList<>();
				for(Level l : job.levelsToShuffle) { keysToDelete.addAll(l.getDeletions(job.reqId)); }
				
				for(Level l : job.shuffleToLevels)	{ l.increasePrefix(); }
				
				final long reqId = job.reqId;
				submitExecutorTask(new Runnable() 
				{
					@Override
					public void run() 
					{
						try { Thread.sleep(5000); } catch (InterruptedException e) {Errors.error(e); }
						
						for(String k : keysToDelete) { s.deleteObject(new DeleteOperation(reqId, k)); }
					}
				});
			}
		
			/*--------*/ partition.unlock(true); /*--------*/
			
			// -d-
			//{ log.append("[OSC (doLocalShuffle)] Job on partition " + partition.getIdx() + ", done with local shuffle, moving to writing...", Log.TRACE); }

			job.step = ShufflingStep.Writing; // move to the local shuffle step
			
			internalEnqueueJob(job);
		}
		
		private void doWriting(final ShuffleJob job, AtomicBoolean someWorkDone)
		{
			if(job.workToDo <= 0) { internalEnqueueJob(job); return; } // no work to be done

			
			someWorkDone.set(true); // we did something

			
			// 4. Cache-out
			// for each block to be cached out:
			final List<ScheduledOperation> pendingCacheOuts = new ArrayList<ScheduledOperation>();
			final List<Entry<Partition.Level, Integer>> cacheOutsToRemove = new ArrayList<Entry<Partition.Level, Integer>>();
			
			boolean allPendingCachedOut = true;
			do
			{
				allPendingCachedOut = true;
				for(Partition.Level level : job.shuffleToLevels)
				{	
					if(level.remainingCacheOuts() == 0) { continue; }
					allPendingCachedOut = false;
					
					if(job.workToDo == 0) { break; }
						
					// issue a cache-out request
					Entry<ScheduledOperation, Integer> entry = level.cacheOutNextBlock(job.reqId);
					
					ScheduledOperation sop = entry.getKey();
					Errors.verify(sop != null);
					
					{
						int offset = entry.getValue();
						pendingCacheOuts.add(sop); job.workToDo--; 
						cacheOutsToRemove.add(new AbstractMap.SimpleEntry<Partition.Level, Integer>(level, offset));
						// -d-
						//{ log.append("[OSC (doWriting)] Job on partition " + job.partition.getIdx() + " started cache-out: (" + level.getIdx() + ", " + offset + ")", Log.TRACE); }
					}
				}
				if(allPendingCachedOut == true) { break; }
			}
			while(job.workToDo > 0);
			
			// -d-
			//{ log.append("[OSC (doWriting)] Job on partition " + job.partition.getIdx() + ", pending cache-outs: " + pendingCacheOuts.size() + " (allPendingCachedOut: " + allPendingCachedOut + ", job.workToDo: " + job.workToDo + ")", Log.TRACE); }
			
			Runnable r = new Runnable() 
			{
				@Override
				public void run() 
				{
					Pollable.waitForCompletion(pendingCacheOuts);	
					while(cacheOutsToRemove.isEmpty() == false)
					{
						Entry<Partition.Level, Integer> entry = cacheOutsToRemove.remove(0);
						Partition.Level level = entry.getKey();
						int offset = entry.getValue();
						
						level.removeCacheOut(offset);
						
						// -d-
						//{ log.append("[OSC (doWriting-run)] Job on partition " + job.partition.getIdx() + ", cached-out: (" + level.getIdx()  + ", " + offset + ")", Log.TRACE); }
					}
					
					unassignedStorageWork.addAndGet(pendingCacheOuts.size()); // increase the remaining storage work capacity
					
					boolean allCachedOut = true;
					for(Partition.Level level : job.shuffleToLevels) { if(level.inProgressCacheOuts() > 0) { allCachedOut = false; } }
					
					// -d-
					//{ log.append("[OSC (doWriting-run)] Job on partition " + job.partition.getIdx() + ", done waiting for cache-outs, allCachedOut: " + allCachedOut, Log.TRACE); }
					
					if(allCachedOut == true) { job.step = ShufflingStep.Finalize; } // move to the done step
					else if(job.workToDo == 0 && unassignedWork.get() == 0) 
					{ 
						Errors.warn("[OSC (doWriting-run)] Job on partition " + job.partition.getIdx() + " we are starving!!");
					}
					internalEnqueueJob(job);
				}
			};
			
			submitExecutorTask(r);			
		}
		
		private synchronized void doFinalize(ShuffleJob job, AtomicBoolean someWorkDone) 
		{
			Partition partition = job.partition;
			
			
			someWorkDone.set(true); // we did something

			// -d-
			//{ log.append("[OSC (doFinalize)] Job on partition " + partition.getIdx() + " is done, decrementing latch.", Log.TRACE); }
			
			//partition.decreaseShufflingJobSize(job.jobSize);
			partition.atomicResetShuffling();
			
			for(CountDownLatch l : job.latches)
			{
				l.countDown(); // decrements latch
				//{ log.append("[OSC (doFinalize)] Job on partition " + partition.getIdx() + " counted down on latch: " + l + " (count: " + l.getCount() + ")", Log.TRACE); }
			}
			ShuffleJob active = activeJobs[partition.getIdx()];
			Errors.verify(active == job);
			
			ShuffleJob next = job.next;
			activeJobs[partition.getIdx()] = next;
			
			if(next != null)
			{
				BlockingQueue<ShuffleJob> setupQueue = queue.get(ShufflingStep.Setup);
				try { setupQueue.put(next); } catch (InterruptedException e) { Errors.error(e); }
				Errors.verify(next.step == ShufflingStep.Setup);
			}
		}
		
		private void internalEnqueueJob(ShuffleJob job) 
		{
			// put the job back		
			//{ log.append("[OSC (enqueueJob)] Enqueuing job on partition " + job.partition.getIdx() + " for step: " + job.step, Log.TRACE); }
			
			BlockingQueue<ShuffleJob> correctQueue = queue.get(job.step);
			try { correctQueue.put(job); } catch (InterruptedException e) { Errors.error(e); }
		}
	}
	
	protected class ProcessRequestTask extends Pollable implements Callable<Void>, Completable
	{		
		protected boolean done = false;
		protected long scheduleTime = 0;
		
		protected ScheduledRequest sreq = null;
		protected byte[] data = null;
		
		protected String jobKey = null;
		protected int blockId = -1;
		protected Position oldPos = null;
		protected Position newPos = null;
		
		protected int evictions = 0;
		
		protected BlockDataItem cachedBDI = null;
		
		protected CountDownLatch evictionLatch = null;
		
		public ProcessRequestTask(ScheduledRequest s, int ev, long st) { sreq = s; evictions = ev; data = null; scheduleTime = st; }
		
		@Override
		public Void call() throws Exception 
		{		
			Errors.verify(sreq != null);
			final Request req = sreq.getRequest();
			final long reqId = req.getId();
			
			long doneScheduleTime = System.currentTimeMillis();
			long elapsedSchedule = doneScheduleTime - scheduleTime;
			{ log.append("[OSC (schedule)] Scheduling time for request " + req.getId() + ": " + elapsedSchedule + "ms.", Log.TRACE); }
			
			
			// fetch the next data access request for blockId and lookup the position map
			blockId = Integer.parseInt(req.getKey()); data = null;
			if(req.getType() == RequestType.PUT) { data = ((PutRequest)sreq.getRequest()).getValue().getData(); }
			
			jobKey = blockId + "";
			
			// get the old position of the block and prepare a (random) new one
			newPos = new Position(rng.nextInt(partitions.size()));
			oldPos = getBlockPos(blockId); Errors.verify(oldPos != null);
			
			final int oldPartitionIdx = oldPos.partitionIdx; 
			
			// -d- 
			//{ log.append("[OSC (schedule)] Will move block " + blockId + " from partition " + oldPartitionIdx + " to " + newPos.partitionIdx + " (req: " + reqId + ")", Log.TRACE);}
			
			// lookup cache slot to see if block is already there
			cachedBDI = dequeueFromEvictionCache(oldPartitionIdx, blockId);
			
			// -d-
			/*if(cachedBDI != null){ log.append("[OSC] Block " + blockId + " was found in eviction cache", Log.TRACE);}
			else { log.append("[OSC (schedule)] Will get block " + blockId + " from pos " + oldPos, Log.TRACE); }
			 */
			
			final int blockIdToRead = (cachedBDI == null) ? blockId : -1;
			
			final Completable callback = this;
			
			// asynchronous call
			readPartition(reqId, oldPartitionIdx, blockIdToRead, callback);
		
			return null;
		}

		// readPartition callbacks
		@Override
		public void onSuccess(DataItem d) 
		{
			long completionCallTime = System.currentTimeMillis();
			
			int newPartitionIdx = newPos.partitionIdx;
			int oldPartitionIdx = oldPos.partitionIdx;
			
			Errors.verify(blockId >= 0 && blockId < clientParams.maxBlocks);
			
			BlockDataItem bdi = null;
			if(cachedBDI != null) { bdi = cachedBDI; }
			
			if(d != null)
			{
				bdi = new CacheDataItem(new EncryptedDataItem(d)).getBlock();
				ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader();
			}
			if(bdi == null){ log.append("[OSC] BDI is null -- job " + jobKey + " (req: " + sreq.getRequest().getId() + "): cachedBDI == null: " + (cachedBDI == null) + ", d == null: " + (d == null), Log.TRACE);}
			
			sreq.onSuccess((data != null) ? new EmptyDataItem() : new SimpleDataItem(bdi.getPayload()));
			
			Errors.verify(bdi != null);
			if(data != null) { bdi.setPayload(data); } // overwrite block if this is a write request
			
			// store the block to the eviction cache
			setBlockPos(blockId, newPos); // first update pos map, to avoid concurrency issues
			addToEvictionCache(newPartitionIdx, bdi);

			evictionLatch = scheduleEvictions(sreq.getRequest().getId(), oldPartitionIdx, evictions);
		
			// complete the job
			{ log.append("[OSC (schedule)] Completing job " + jobKey + " (req: " + sreq.getRequest().getId() + ")", Log.TRACE);}
			
			jobsQueue.complete(jobKey);
			
			long completedTime = System.currentTimeMillis();
			long elapsedDone = completedTime - completionCallTime;
			{ log.append("[OSC (schedule)] Done time for request " + sreq.getRequest().getId() + ": " + elapsedDone + "ms.", Log.TRACE); }
			
			
			done = true;
		}

		@Override
		public void onFailure() 
		{
			Errors.verify(true == false, "Coding FAIL!");
		}

		public CountDownLatch getEvictionLatch() { return evictionLatch; }

		@Override
		public boolean isReady() { return done; }
	}
	
	public ObliviStoreClient()
	{
		this(defaultMaxSimultaneousRequests);
	}
	
	public ObliviStoreClient(int simultaneousReqs)
	{
		super(); 
		maxSimultaneousRequests = (simultaneousReqs > 0) ? simultaneousReqs : defaultMaxSimultaneousRequests;
	}
	
	protected boolean synchronous = false;
	//protected boolean synchronous = true;
	
	protected List<Partition> partitions = new ArrayList<Partition>();
	
	protected List<CacheSlot> evictionCache = new ArrayList<CacheSlot>();
	
	protected PositionMap posMap = null;
	
	private static final int maxThreadCount = 128;
	
	protected ExecutorService executor = null;
	
	protected int maxSimultaneousRequests = -1;
	private static final int defaultMaxSimultaneousRequests = 3000; // as in ObliviStore C# code
	
	protected int maxPartitionSize = 0;
	protected int levelsPerPartition = 0;
	
	protected int topLevelRealBlocksCount = 0;
	
	protected Semaphore earlyCacheInsSemaphore = null;
	protected Semaphore evictionSemaphore = null;
	
	protected int shufflingWorkPerRequest = 0;
	
	private static final double evictionRate = 1.25; // as in ObliviStore C# code

	private static final int dummyBlockId = -1;

	protected boolean doPiggyBackEvictions = true; // as in ObliviStore C# code
	
	protected int nextPartitionIdx = 0;
	
	protected int evictionCacheNumItems = 0;
	protected int evictionCachePeakSize = 0;
	
	//// protected BlockingQueue<ShuffleJob> shuffleJobsQueue = new LinkedBlockingQueue<ShuffleJob>();
	
	protected Shuffler shuffler = null;
	
	protected Lock localStateLock = new ReentrantLock();
	
	protected Map<Integer, Entry<AtomicInteger, Semaphore>> ownershipMap = new HashMap<Integer, Entry<AtomicInteger, Semaphore>>();

	// just as ObliviStore C# implementation -> only 1 thread to avoid concurrency issues
	protected ThreadedJobsQueue jobsQueue = new SingleThreadJobsQueue(); 
	
	private Future<?> submitExecutorTask(Runnable r) 
	{
		//////////////////////////////////////////////////////////////////////
		/* ---------------- */ localStateLock.lock(); /* ------------------ */
		Future<?> f = executor.submit(r);
		/* --------------- */ localStateLock.unlock(); /* ----------------- */
		//////////////////////////////////////////////////////////////////////
		
		return f;
	}
	
	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		synchronous = is.readBoolean();
		
		// load partitions
		int numPartitions = is.readInt();
		for(int partitionIdx = 0; partitionIdx < numPartitions; partitionIdx++) { partitions.add(new Partition(is, s, rng)); }
		
		// load the eviction cache
		for(int partitionIdx = 0; partitionIdx < numPartitions; partitionIdx++) { evictionCache.add(new CacheSlot(is)); }
		
		// load the position map
		posMap = new PositionMap(is);
		
		maxPartitionSize = is.readInt();
		levelsPerPartition = is.readInt();
		topLevelRealBlocksCount = is.readInt();
		
		evictionCacheNumItems = is.readInt();
		evictionCachePeakSize = is.readInt();
		
		shufflingWorkPerRequest = is.readInt();
		
		doPiggyBackEvictions = is.readBoolean();
		
		nextPartitionIdx = is.readInt();
	}
	
	
	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		os.writeBoolean(synchronous);
		
		// save the partitions
		os.writeInt(partitions.size());
		for(Partition partition : partitions) { partition.save(os); }
		
		// save the eviction cache
		for(CacheSlot slot : evictionCache) { slot.save(os); }

		// save the position map
		posMap.save(os);
		
		os.writeInt(maxPartitionSize);
		os.writeInt(levelsPerPartition);
		os.writeInt(topLevelRealBlocksCount);
		
		os.writeInt(evictionCacheNumItems);
		os.writeInt(evictionCachePeakSize);
		
		os.writeInt(shufflingWorkPerRequest);
		
		os.writeBoolean(doPiggyBackEvictions);
		
		os.writeInt(nextPartitionIdx);
	}
	
	// override init and shutdown
	@Override
	protected void init(boolean reset)
	{
		rng  = new SecureRandom();
		
		executor = Executors.newFixedThreadPool(maxThreadCount);
		
		Errors.verify(clientParams.maxBlocks <= Integer.MAX_VALUE, "Can't support that many blocks!");
		
		if(reset == true)
		{
			nextPartitionIdx = 0;
			
			shufflingWorkPerRequest = (int)(2 * Math.floor(Math.log(clientParams.maxBlocks)/Math.log(2.0))); // as in ObliviStore code
			
			posMap = new PositionMap((int)clientParams.maxBlocks);
			
			// number of partitions, capacity and size of each partition calculated just as in ObliviStore code
			int numPartitions = (int)Math.ceil(Math.sqrt(clientParams.maxBlocks));
			int expectedPartitionBlockCount = (int)Math.ceil(clientParams.maxBlocks / numPartitions);
			
			int log2PartitionCapacity = (int)Math.floor(Math.log(expectedPartitionBlockCount)/Math.log(2.0));
			int extraCapacityCount = expectedPartitionBlockCount - (1 << log2PartitionCapacity);
	
			double extraCapacityFraction = calculateExtraCapacityFraction(numPartitions);
			int partitionCapacity = (int)(((1 << log2PartitionCapacity) + extraCapacityCount) * (1.0 + extraCapacityFraction));
			
			log2PartitionCapacity = (int)Math.floor(Math.log(expectedPartitionBlockCount)/Math.log(2.0));
			levelsPerPartition = log2PartitionCapacity + 1;
			
			topLevelRealBlocksCount = partitionCapacity;
			
			List<ArrayList<Integer>> itemsPerPartition = new ArrayList<ArrayList<Integer>>();
			for(int i=0; i < numPartitions; i++)
			{
				partitions.add(new Partition(s, i, levelsPerPartition, topLevelRealBlocksCount, rng));
				evictionCache.add(new CacheSlot());
				itemsPerPartition.add(new ArrayList<Integer>());
			}
		
		
			// initial block placement (fastinit)
			SessionState ss = SessionState.getInstance();
			boolean fastInit = ss.fastInit;
			
			Map<String, Request> fastInitMap = ss.fastInitMap;
			
			maxPartitionSize = 0;
			{
				Errors.verify(partitions.size() == itemsPerPartition.size());
				for(int blockId = 0; blockId < clientParams.maxBlocks; blockId++)
				{
					int partitionIdx = rng.nextInt(partitions.size());
					itemsPerPartition.get(partitionIdx).add(blockId);
					setBlockPos(blockId, new Position(partitionIdx, Position.ZeroBlock)); // if fastInit is null, the blocks will be zero blocks
				}
				
				for(Partition partition : partitions)
				{
					maxPartitionSize = Math.max(maxPartitionSize, partition.getSize());
					
					List<Integer> blockIds = itemsPerPartition.get(partition.getIdx());
					List<BlockDataItem> uploadsBuffer = null;
					if(fastInit == true)
					{
						uploadsBuffer = new ArrayList<BlockDataItem>();
						for(int blockId : blockIds)
						{
							DataItem di = null; String blockIdStr = "" + blockId;
							if(fastInitMap != null && fastInitMap.containsKey(blockIdStr) == true)
							{
								Request req = fastInitMap.get(blockIdStr);
								Errors.verify(req.getType() == RequestType.PUT);
								PutRequest put = (PutRequest)req;
								byte[] val = put.getValue().getData();
								if(val.length < clientParams.contentByteSize) { val = Arrays.copyOf(val, clientParams.contentByteSize); }
								di = new SimpleDataItem(val);
							}
							else { di = new IntegerSeededDataItem(blockId, clientParams.contentByteSize); }
							
							BlockDataItem bdi = new BlockDataItem(new ObliviStoreHeader(blockId), di.getData());
							uploadsBuffer.add(bdi);
						}
					}
					
					Map<Integer, Map.Entry<Integer, Integer>> m  = partition.initialize(rng, fastInit, blockIds, uploadsBuffer, getDummyBlock());
					
					if(fastInit == true) 
					{ 
						List<ScheduledOperation> pendingUploads = new ArrayList<ScheduledOperation>(); int count = 0;
						Level[] levels = partition.getLevels();
						for(Level level : levels)
						{
							Errors.verify(level.dummyInit() == false);
							if(level.isEmpty() == true) { continue; }
							
							while(level.remainingCacheOuts() > 0)
							{
								Entry<ScheduledOperation, Integer> entry = level.cacheOutNextBlock(Request.initReqId);
								ScheduledOperation sop = entry.getKey();
								if(sop != null) { pendingUploads.add(sop); count++; }
							
								level.removeCacheOut(entry.getValue()); // remove immediately
							}
							
							Pollable.removeCompleted(pendingUploads);
						}
						// update the position map
						for(int blockId : m.keySet())
						{
							Entry<Integer, Integer> entry = m.get(blockId);
							int levelIdx = entry.getKey();
							int offset = entry.getValue();
							
							Position newPos = new Position(partition.getIdx(), levelIdx, offset);
							setBlockPos(blockId, newPos);
						}
						
						Pollable.waitForCompletion(pendingUploads);
						{ log.append("[OSC (init)] Uploaded " + count + " blocks (" + blockIds.size() + " real) to partition " + partition.getIdx(), Log.TRACE); }
					}
				}
			}
		}
		
		// init semaphores (same as in C# implementation)
		earlyCacheInsSemaphore = new Semaphore(maxSimultaneousRequests);
		evictionSemaphore = new Semaphore((int)Math.ceil(evictionRate * maxSimultaneousRequests));
		
		shuffler = new Shuffler(); // init the shuffler

		{ log.append("[OSC (init)] done.", Log.TRACE); }
	}
	
	// from ObliviStore code
	public static double calculateExtraCapacityFraction(int numPartitions) 
	{
		int log2PartitionCount = (int)Math.ceil(Math.log(numPartitions)/Math.log(2.0));
		if (log2PartitionCount <= 4 / 2) { return 4.0; }
		else if (log2PartitionCount <= 6 / 2) { return 3.0; }
		else if (log2PartitionCount <= 10 / 2) { return 1.0; }
		else if (log2PartitionCount <= 12 / 2) { return 0.6; }
		else if (log2PartitionCount <= 14 / 2) { return 0.4; }
		else if (log2PartitionCount <= 16 / 2) { return 0.33; }
		else if (log2PartitionCount <= 18 / 2) { return 0.28; }
		else if (log2PartitionCount <= 20 / 2) { return 0.23; }
		else { return 0.2; }
	}

	@Override
	protected void shutdown()
	{
		// make sure we assign enough work so all the eviction jobs can finish
		shuffler.assignWork(partitions.size() * maxPartitionSize * 2 * 2); 
		
		shuffler.shutdown();
		
		try { Thread.sleep(100); } catch (InterruptedException e) { Errors.error(e); }
		
		executor.shutdown();
		
		try { Thread.sleep(50); } catch (InterruptedException e) { Errors.error(e); }
		
		jobsQueue.shutdown();
		
		log.append("[OSC] Waiting for all tasks to terminate...", Log.INFO);
		
		try { executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); } catch (InterruptedException e) { Errors.error(e); }
		
		log.append("[OSC] Done waiting, all tasks have completed.", Log.INFO);
	}

	@Override
	public boolean isSynchronous() { return synchronous; }

	@Override
	public String getName() { return "ObliviStore(" + synchronous + ")"; }

	private ScheduledRequest schedule(Request req)
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		int blockId = Integer.parseInt(req.getKey());
		
		{ log.append("[OSC (schedule)] scheduling request " + req.getId() + ", blockId: " + blockId, Log.TRACE); }
		
		long startTime = System.currentTimeMillis();
		
		// decrement the early cache-in semaphore by the number of levels
		try { earlyCacheInsSemaphore.acquire(levelsPerPartition); } catch (InterruptedException e) { Errors.error(e); }
		
		// -d-
		{ log.append("[OSC (schedule)] Done waiting on earlyCacheInsSemaphore for req " + req.getId() + ": " + (System.currentTimeMillis() - startTime) + "ms.", Log.TRACE); }
		
		// decrement the eviction semaphore by eviction rate
		int evictions = getRandomIntegerEvictionCount();
		try { evictionSemaphore.acquire(evictions); } catch (InterruptedException e) { Errors.error(e); }
		
		long elapsed = System.currentTimeMillis() - startTime;
		{ log.append("[OSC (schedule)] Semaphore waiting time for request " + req.getId() + ": " + elapsed + "ms.", Log.TRACE); }
		
		ProcessRequestTask task = new ProcessRequestTask(sreq, evictions, System.currentTimeMillis());
		
		Job<Void> job = new Job<Void>("schedule-"+req.getId()+"--" + blockId, task);
		jobsQueue.scheduleJob(blockId + "", job, SchedulingPriority.HIGH);
		
		if(synchronous == true) 
		{ 
			{ log.append("[OSC (schedule)] waiting for request " + req.getId() + " to finish.", Log.TRACE); }
			
			Set<Pollable> s = new HashSet<Pollable>(); s.add(task);
			Pollable.waitForCompletion(s);
			
			CountDownLatch latch = task.getEvictionLatch();
			{ log.append("[OSC (schedule)] waiting for latch on request " + req.getId() + ", count: " + latch.getCount(), Log.TRACE); }
			try 
			{ 
				
				if(latch != null) { latch.await(); }
			} 
			catch (Exception e) { Errors.error(e); } 

		}
		return sreq;
	}
	
	@Override
	public ScheduledRequest scheduleGet(GetRequest req) { return schedule(req); }

	@Override
	public ScheduledRequest schedulePut(PutRequest req) { return schedule(req); }

	public BlockDataItem getDummyBlock() 
	{
		return new BlockDataItem(new ObliviStoreHeader(dummyBlockId), encodingUtils.getDummyBytes(clientParams.contentByteSize));
	}

	private Position getBlockPos(int blockId) 
	{
		//////////////////////////////////////////////////////////////////////
		/* ---------------- */ localStateLock.lock(); /* ------------------ */
		Position pos = posMap.getPos(blockId);
		// -d-
		//{ log.append("[OSC (getBlockPos)] Block " + blockId + " is at " + pos, Log.TRACE); }
		/* --------------- */ localStateLock.unlock(); /* ----------------- */
		//////////////////////////////////////////////////////////////////////
		return pos;
	}
	
	private void setBlockPos(int blockId, Position pos)
	{
		//////////////////////////////////////////////////////////////////////
		/* ---------------- */ localStateLock.lock(); /* ------------------ */
		posMap.setPos(blockId, pos);
		// -d-
		//{ log.append("[OSC (setBlockPos)] Block " + blockId + " -> " + pos, Log.TRACE); }
		/* --------------- */ localStateLock.unlock(); /* ----------------- */
		//////////////////////////////////////////////////////////////////////
	}

	private void addToEvictionCache(int idx, BlockDataItem bdi) 
	{
		//////////////////////////////////////////////////////////////////////
		/* ---------------- */ localStateLock.lock(); /* ------------------ */
		
		evictionCacheNumItems++;
		if(evictionCachePeakSize < evictionCacheNumItems) { evictionCachePeakSize = evictionCacheNumItems; }
		
		Errors.verify(idx < evictionCache.size());
		CacheSlot slot = evictionCache.get(idx); Errors.verify(slot != null);	
		
		// -d-
		//{ log.append("[OSC (addToEvictionCache)] Storing block " + ((ObliviStoreHeader)bdi.getHeader()).getBlockId() + " in slot " + idx, Log.TRACE); }
		
		slot.add(bdi);
		
		/* --------------- */ localStateLock.unlock(); /* ----------------- */
		//////////////////////////////////////////////////////////////////////
	}

	private BlockDataItem dequeueFromEvictionCache(int idx, int blockId) 
	{
		//////////////////////////////////////////////////////////////////////
		/* ---------------- */ localStateLock.lock(); /* ------------------ */
		
		Errors.verify(idx < evictionCache.size());
		CacheSlot slot = evictionCache.get(idx); Errors.verify(slot != null);	
		BlockDataItem bdi = slot.dequeue(blockId); // returns null if block isn't found
		
		if(bdi != null) 
		{
			evictionCacheNumItems--;
			// -d-
			//{ log.append("[OSC (dequeueFromEvictionCache)] Removed block " + ((ObliviStoreHeader)bdi.getHeader()).getBlockId() + " from slot " + idx, Log.TRACE); }
		}
		
		/* --------------- */ localStateLock.unlock(); /* ----------------- */
		//////////////////////////////////////////////////////////////////////
		
		return bdi;
	}
	
	private List<BlockDataItem> getAndPadFromEvictionCache(int idx, int jobSize) 
	{
		List<BlockDataItem> ret = new ArrayList<BlockDataItem>();
		//////////////////////////////////////////////////////////////////////
		/* ---------------- */ localStateLock.lock(); /* ------------------ */
		
		Errors.verify(idx < evictionCache.size());
		CacheSlot slot = evictionCache.get(idx);
		ret.addAll(slot.removeAndPad(jobSize));
		
		/* --------------- */ localStateLock.unlock(); /* ----------------- */
		//////////////////////////////////////////////////////////////////////
		
		return ret;
	}
	
	private CountDownLatch scheduleEvictions(long reqId, int piggyBackPartitionIdx, int count) 
	{	
		List<Integer> partitionsForEviction = new ArrayList<Integer>();
		
		partitionsForEviction.add(piggyBackPartitionIdx);
		for(int i=0; i<count-1; i++)
		{
			partitionsForEviction.add(nextPartitionIdx); // sequential evictor
			nextPartitionIdx = (nextPartitionIdx + 1) % partitions.size();
		}
		
		CountDownLatch retLatch = new CountDownLatch(partitionsForEviction.size());
		for(int partitionIdx : partitionsForEviction)
		{
			Partition partition = partitions.get(partitionIdx);
			
			// -d- 
			//{ log.append("[OSC (scheduleEvictions)] Adding ShuffleJob for req " + reqId + ", partition: " + partition.getIdx() + ", latch: " + retLatch, Log.TRACE); }
			
			shuffler.addJob(reqId, partition, retLatch);
			
			//Errors.verify(shuffleTaskThread.isAlive() == true);
			
			// assign shuffling work (if synchronous, make sure we assign enough so we don't risk starving the shuffling)
			final int maxWorkPerPartiton = partition.getSize() + topLevelRealBlocksCount * 2;
			if(synchronous == true && shuffler.unassignedWork() < partitionsForEviction.size() * maxWorkPerPartiton) 
			{ shuffler.assignWork(maxWorkPerPartiton);	}
			else { shuffler.assignWork(); }
		}
		return retLatch;
	}

	private int getRandomIntegerEvictionCount()
	{
		// on average, get 'rate' count
		double rate = (doPiggyBackEvictions == true) ? evictionRate - 1.0 : evictionRate;
		double floorRate = Math.floor(rate);
		
		int ret = (int)floorRate + (((rate - floorRate) < rng.nextDouble()) ? 1 : 0);
		return (doPiggyBackEvictions == true) ? ret + 1 : ret;
	}
	
	private Set<Integer> getRandomPartitions(int piggyBackPartitionIdx, int count) 
	{
		Errors.verify(count <= partitions.size());
		Set<Integer> ret = new HashSet<Integer>();
	
		 // if do piggy back eviction, make sure we add piggyBackPartitionIdx
		if(doPiggyBackEvictions == true){ ret.add(piggyBackPartitionIdx); }
		Errors.verify(count > 0 || doPiggyBackEvictions == false);
		
		while(ret.size() < count) { ret.add(rng.nextInt(partitions.size())); }
		return ret;
	}
	
	public void readPartition(long reqId, int partitionIdx, final int blockIdToRead, final Completable callback) 
	{
		Errors.verify(partitionIdx != Position.InvalidPartitionIdx && partitionIdx < partitions.size());
		Partition partition = partitions.get(partitionIdx);
		
		/* ------ */ partition.lock(false); /* ------ */ // lock partition for reading
		
		// 1) looks up the position map to determine the level l* where the block resides.
		// if the requested block is dummy or blockId is not in partition pos.partitionIdx, then l* = \bot
		Position pos = blockIdToRead >= 0 ? posMap.getPos(blockIdToRead) : null;
		final Partition.Level levelStar = (pos != null) ? partition.getLevel(pos.levelIdx) : null;
		
		List<Partition.Level> levelsToReadFrom = new ArrayList<Partition.Level>();
		for(Partition.Level level : partition.getLevels())
		{
			// increment early cache-ins semaphore by 1 if any of the following condition are satisfied:
			// - level is empty
			// - level is not marked for shuffling
			// - level is marked for shuffling but all blocks have been cached in
			boolean empty = level.isEmpty();
			boolean markedForShuffling = level.isMarkedForShuffling();
			int unreadBlocks = level.unreadBlocksForShuffling();
			boolean incSemaphore = ((empty == true) || (markedForShuffling == false) || (markedForShuffling == true && unreadBlocks <= 0));
		
			if(incSemaphore == true) { earlyCacheInsSemaphore.release(1); }
			
			if(empty == false && level.dummyInit() == false) { levelsToReadFrom.add(level); }
		}
		
		//// Errors.verify(levelStar == null || levelStar.isEmpty() == false);
		
		BlockDataItem bdi2 = null;
		if(pos != null && pos.levelIdx == Position.ZeroBlock)
		{
			Errors.verify(blockIdToRead >= 0 && blockIdToRead < clientParams.maxBlocks);
			bdi2 = getDummyBlock();
			bdi2.setHeader(new ObliviStoreHeader(blockIdToRead));
			byte[] val = bdi2.getPayload(); Arrays.fill(val, (byte)0);
			bdi2.setPayload(val);
			if(pos != null) { log.append("[OSC (readPartition)] Created zero block " + blockIdToRead, Log.TRACE); }
		}
		final BlockDataItem bdi = bdi2;
		
		ScheduledOperation tmpSOp = null;
		
		// for each filled level (i.e., each non-empty level)
		final List<ScheduledOperation> pendingDownloads = new ArrayList<ScheduledOperation>();
		for(Partition.Level level : levelsToReadFrom)
		{
			Errors.verify(level.isEmpty() == false);
			if(level == levelStar) // ReadReal
			{
				Errors.verify(pos.offsetInLevel >= 0 && pos.offsetInLevel < level.getSize());
				
				tmpSOp = level.readBlock(reqId, pos.offsetInLevel);
				Errors.verify(tmpSOp != null);
				pendingDownloads.add(tmpSOp);
			}
			else // ReadFake
			{
				ScheduledOperation sop = level.readFake(reqId);
				if(sop != null) { pendingDownloads.add(sop); }
			}
		}
		final ScheduledOperation realBlockScheduledOp = tmpSOp;
		
		/* ------ */ partition.unlock(false); /* ------ */ // unlock partition
		
		Runnable r = new Runnable()
		{
			@Override
			public void run()
			{
				// to prevent timing channel leakage, must wait for fake in to complete before returning the block
				Pollable.waitForCompletion(pendingDownloads);
						
				//// Errors.verify(levelStar == null || realBlockScheduledOp != null);
			
				DataItem di = null;
				if(realBlockScheduledOp != null) { di = realBlockScheduledOp.getDataItem(); }
				else if(bdi != null) { di = bdi.getEncryption(); }
				else if(blockIdToRead >= 0)
				{
					Errors.verify(levelStar == null && realBlockScheduledOp == null);
					Errors.warn("[OSC (readPartition-runnable)] consistency issue (returning zero), blockIdToRead: " + blockIdToRead + ", levelStar: " + levelStar + ", realBlockScheduledOp: " + realBlockScheduledOp);
					
					// tmp - prevent consistency issues
					{
						BlockDataItem bdi2 = getDummyBlock();
						bdi2.setHeader(new ObliviStoreHeader(blockIdToRead));
						byte[] val = bdi2.getPayload(); Arrays.fill(val, (byte)0);
						bdi2.setPayload(val);
						
						di = bdi2.getEncryption();
					}
				}
				
				callback.onSuccess(di); // callback!
			}
		};
		
		Future<?> f = submitExecutorTask(r);
	}
	
	public long peakByteSize()
	{
		final double bitsPerByte = 8.0;
		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long cacheSize = evictionCachePeakSize * entryByteSize;
		
		int logMaxBlocks = (int)Math.ceil(Math.log(clientParams.maxBlocks)/Math.log(2.0));
		
		int partIdxBits = (int)Math.ceil(Math.log(partitions.size())/Math.log(2.0));
		int levelIdxBits = (int)Math.ceil(Math.log(levelsPerPartition)/Math.log(2.0));
		int offsetBits = (int)Math.ceil(Math.log(topLevelRealBlocksCount)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil((partIdxBits + levelIdxBits + offsetBits)/bitsPerByte);
		long posMapSize = clientParams.maxBlocks * posMapEntrySize;
		
		int partitionStateSize = (int)Math.ceil(levelsPerPartition / bitsPerByte);
		partitionStateSize += (int)Math.ceil(maxPartitionSize / bitsPerByte); // size of blocksRead bitset
		partitionStateSize += clientParams.encryptionKeyByteSize;
		
		long partitionsStateSize = partitions.size() * partitionStateSize;
		
		return cacheSize + posMapSize + partitionsStateSize;
	}
}
