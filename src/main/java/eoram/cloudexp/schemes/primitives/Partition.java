package eoram.cloudexp.schemes.primitives;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.crypto.IntegerPermutation;
import eoram.cloudexp.crypto.SimpleIntegerPermutation;
import eoram.cloudexp.data.BlockDataItem;
import eoram.cloudexp.data.CacheDataItem;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.EncryptedDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.encoding.ObliviStoreHeader;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.ObliviStoreClient;
import eoram.cloudexp.service.DownloadOperation;
import eoram.cloudexp.service.Operation;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.Errors;

/** 
 * Represents an ObliviStore partition 
 */
public class Partition 
{	
	protected Log log = Log.getInstance();
	
	public class Level 
	{
		protected class CacheInBuffer
		{
			protected Map<Integer, ScheduledOperation> map = new HashMap<Integer, ScheduledOperation>();
			
			protected CacheInBuffer() {}
			
			public synchronized void clear() { map.clear(); }
			public synchronized ScheduledOperation remove(int offset) { return map.remove(offset); }
			public synchronized void add(int offset, ScheduledOperation sop) 
			{				
				Errors.verify(map.containsKey(offset) == false);
				map.put(offset, sop);
			}
			
			public List<BlockDataItem> drainAll() 
			{
				List<BlockDataItem> ret = new ArrayList<BlockDataItem>();
				
				for(int offset : map.keySet())
				{
					ScheduledOperation sop = map.get(offset);
					
					DataItem encryptedSDI = new SimpleDataItem(sop.getDataItem().getData());
					BlockDataItem bdi = new CacheDataItem(new EncryptedDataItem(encryptedSDI)).getBlock();
					ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader();
					
					if(header.getBlockId() >= 0) { ret.add(bdi); }
				}
				clear();				
				return ret;
			}
			
			protected void save(ObjectOutputStream os) throws Exception
			{
				Pollable.waitForCompletion(map.values()); // make sure all pending ops complete
				
				os.writeInt(map.size());
				for(int blockId : map.keySet())
				{
					os.writeInt(blockId);
					ScheduledOperation sop = map.get(blockId);
					Errors.verify(sop != null && sop.isReady() == true);
					sop.save(os);
				}
			}
			
			protected CacheInBuffer(ObjectInputStream is) throws Exception
			{
				int size = is.readInt();
				while(size > 0)
				{
					int blockId = is.readInt();
					ScheduledOperation sop = new ScheduledOperation(is);
					map.put(blockId, sop);
					size--;
				}
			}
		}
		
		protected class CacheOutBuffer
		{
			protected Map<Integer, BlockDataItem> preparedMap = new TreeMap<Integer, BlockDataItem>();
			protected Map<Integer, BlockDataItem> pendingMap = new TreeMap<Integer, BlockDataItem>();
			
			public CacheOutBuffer() { }
			
			public synchronized void clear() { preparedMap.clear(); pendingMap.clear(); }
			public synchronized BlockDataItem remove(int offset) 
			{
				if(preparedMap.containsKey(offset) == true) { return preparedMap.remove(offset); }
				else { return pendingMap.remove(offset); }
			}
			public synchronized BlockDataItem removePending(int offset) { return pendingMap.remove(offset); }
			
			// add to preparedMap
			public synchronized void addPrepared(int offset, BlockDataItem bdi) 
			{
				Errors.verify(preparedMap.containsKey(offset) == false);
				preparedMap.put(offset, bdi);
			}
			// get and remove from preparedMap (and add to pendingMap)
			public synchronized Entry<Integer, BlockDataItem> nextCacheOut() 
			{
				Iterator<Entry<Integer, BlockDataItem>> iter = preparedMap.entrySet().iterator();
				if(iter.hasNext() == false) { return null; }
				
				Entry<Integer, BlockDataItem> ret = iter.next();
				
				pendingMap.put(ret.getKey(), ret.getValue());
				iter.remove(); // remove the entry from the map (use iterator)
				
				return ret;
			}
			public synchronized int getPreparedSize() { return preparedMap.size(); }
			
			public synchronized int getPendingSize() { return pendingMap.size(); }
			
			protected void save(ObjectOutputStream os) throws Exception 
			{
				ObliviStoreClient.saveBlocksMap(os, preparedMap); // save preparedMap
				ObliviStoreClient.saveBlocksMap(os, pendingMap); // save pendingMap
			}
			
			protected CacheOutBuffer(ObjectInputStream is) throws Exception 
			{
				ObliviStoreClient.loadBlocksMap(is, preparedMap); // load preparedMap
				ObliviStoreClient.loadBlocksMap(is, pendingMap); // load pendingMap
			}

			public synchronized BlockDataItem lookup(int offset) 
			{ 
				BlockDataItem ret = preparedMap.get(offset);
				if(ret != null) { Errors.verify(pendingMap.get(offset) == null); return ret; }
				
				return pendingMap.get(offset); 
			}
		}
		
		protected Random rng = null;
		
		private static final int ShufflingNotScheduled = -2;
		
		protected int partition = 0;
		protected int idx = 0;
		protected int realBlocks = 0;
		protected int dummyBlocks = 0;
		protected int realBlocksReadCount = 0;
		protected int dummyBlocksReadCount = 0;
		protected boolean empty = false;
		protected boolean dummyInit = false;
		protected boolean markedForShuffling = false;
		protected int shufflingRemainingBlocksToRead = ShufflingNotScheduled;
		
		protected BitSet blocksRead = null;
		protected IntegerPermutation permutation = null;
		
		protected CacheInBuffer cacheInBuffer = new CacheInBuffer();
		protected CacheOutBuffer cacheOutBuffer = new CacheOutBuffer();
		
		public Level(int p, int i, int capacity, Random r) 
		{ 		
			rng = r;
			
			partition = p;
			idx = i; 
			
			pref = 0; // -d-
			
			realBlocks = capacity; 
			dummyBlocks = capacity;
			blocksRead = new BitSet(totalBlocks());
			clear();
		}

		protected synchronized void clear() 
		{
			realBlocksReadCount = 0;
			dummyBlocksReadCount = 0;
			empty = true;
			dummyInit = false;
			markedForShuffling = false;
			shufflingRemainingBlocksToRead = ShufflingNotScheduled;
			blocksRead.clear();
		}
		
		public synchronized int getIdx() { return idx; }

		public synchronized boolean isEmpty() { return empty; }

		public synchronized void markForShuffling()
		{
			markedForShuffling = true;
			if(shufflingRemainingBlocksToRead == ShufflingNotScheduled) { shufflingRemainingBlocksToRead = realBlocks; }
			if(dummyInit == true) { shufflingRemainingBlocksToRead = 0; }
		}
		public synchronized boolean isMarkedForShuffling() { return markedForShuffling; }

		public synchronized int unreadBlocksForShuffling() { return shufflingRemainingBlocksToRead == ShufflingNotScheduled ? 0 : shufflingRemainingBlocksToRead; }

		private synchronized int totalBlocks() { return realBlocks + dummyBlocks; }

		public synchronized void setFilled() { empty = false; }

		public synchronized int getSize() { return totalBlocks(); }
		
		//public synchronized int realBlocksCount() { return totalBlocks(); } // for debugging purpose
		public synchronized int realBlocksCount() { return realBlocks; }

		public synchronized ScheduledOperation readBlock(long reqId, int offset) 
		{
			Errors.verify(empty == false);
			
			ScheduledOperation sop = null;
			final ScheduledOperation cachedScheduledOp = getCachedInOp(offset);
			
			if(cachedScheduledOp != null)
			{
				final ScheduledOperation fakeScheduledOp = readFake(reqId);
				if(fakeScheduledOp == null) { sop = cachedScheduledOp; }
				else
				{
					sop = new ScheduledOperation(cachedScheduledOp.getOperation())
					{
						public synchronized boolean isReady() { return fakeScheduledOp.isReady(); }
						public synchronized DataItem getDataItem()
						{
							fakeScheduledOp.waitUntilReady();
							return cachedScheduledOp.getDataItem();
						}
					};
				}
			}
			else
			{
				sop = downloadReal(reqId, offset);
			}
			
			return sop;
		}

		public synchronized ScheduledOperation readFake(long reqId) 
		{
			Errors.verify(empty == false);
			
			ScheduledOperation sop = null;
			
			boolean marked = isMarkedForShuffling();
			
			//if(marked == false)
			if(shufflingRemainingBlocksToRead == ShufflingNotScheduled)
			{
				sop = downloadNextDummy(reqId); // download next dummy block
			}
			else if(shufflingRemainingBlocksToRead > 0)
			{
				sop = readRandomBlockTowardsShuffling(reqId);
			}
			else
			{
				Errors.verify(shufflingRemainingBlocksToRead == 0);
				return null;
			}
			
			return sop;
		}
		
		private ScheduledOperation readRandomBlockTowardsShuffling(long reqId)
		{
			Errors.verify(shufflingRemainingBlocksToRead > 0);
			
			int remainingRealBlocks = realBlocks - realBlocksReadCount;
			boolean readReal = rng.nextInt(shufflingRemainingBlocksToRead) < remainingRealBlocks;
			
			int readIdx = ((readReal == true) ? nextRandomRealIndex() : nextRandomDummyIndex());
			
			return cacheInByIndex(reqId, readIdx);
		}

		public synchronized ScheduledOperation readNextBlockTowardsShuffling(long reqId) 
		{
			ScheduledOperation sop = cacheInNextUnread(reqId); // perform early cache in
			return sop;
		}

		private void markAsRead(int idx)
		{
			Errors.verify(idx >= 0 && idx < totalBlocks());
			blocksRead.set(idx);
			
			if(shufflingRemainingBlocksToRead == ShufflingNotScheduled)
			{
				if(realBlocks <= blocksRead.cardinality()) 
				{ 
					/** Doing things like ObliviStore here looks like we'll leak data, since it could be realBlocks is < totalBlocks()/2 
					 * in which case, we read less than totalBlocks()/2 blocks from this level before reshuffling
					 * this leaks to server that the number of real blocks is less than totalBlocks()/2 **/
					
					// in this case, we do it like in ObliviStore C# implementation, which gives ObliviStore the advantage in the terms of performance
					shufflingRemainingBlocksToRead = realBlocks; // just as in ObliviStore C# implementation
					
					//shufflingRemainingBlocksToRead = totalBlocks()/2;
				}
			}
		}
		
		private void markAsClear(int idx)
		{
			Errors.verify(idx >= 0 && idx < totalBlocks());
			blocksRead.clear(idx);
		}
		
		private ScheduledOperation downloadReal(long reqId, int offset) 
		{		
			if(shufflingRemainingBlocksToRead > 0) { shufflingRemainingBlocksToRead--; }	
			
			int idx = permutation.reverseMap(offset);
			
			// don't mark block as read here, it will be done in 'read()'
			////// markAsRead(idx); // mark real block as read
			
			ScheduledOperation sop = read(reqId, idx, false); Errors.verify(sop != null);
			
			realBlocksReadCount++;
			Errors.verify(blocksRead.cardinality() == realBlocksReadCount + dummyBlocksReadCount);
			
			return sop;
		}

		private ScheduledOperation scheduleDownload(long reqId, int offset) 
		{
			Errors.verify(offset >= 0 && offset < totalBlocks());
			DownloadOperation download = new DownloadOperation(reqId, getKey(offset));
			ScheduledOperation sop = s.downloadObject(download);
			
			return sop;
		}

		protected int pref = -1;
		// -d-
		public synchronized void increasePrefix() { pref++; }
		
		public synchronized List<String> getDeletions(long reqId)
		{
			List<String> ret = new ArrayList<>();
			for(int offset = 0; offset < totalBlocks(); offset++)
			{
				String k = getKey(offset);
				ret.add(k);
			}
			return ret;
		}
		
		private String getKey(int offset) 
		{
			return pref + "----" + partition + "---" + idx + "--" + offset; 
		}

		private ScheduledOperation getCachedInOp(int offset) 
		{
			Errors.verify(offset >= 0 && offset < totalBlocks());
			return cacheInBuffer.remove(offset);
		}
		
		private BlockDataItem getCachedOut(int offset) 
		{
			Errors.verify(offset >= 0 && offset < totalBlocks());
			return cacheOutBuffer.lookup(offset); // don't remove anything
		}

		private ScheduledOperation read(long reqId, int idx, boolean cacheIn)
		{
			ScheduledOperation sop = null;
			
			Errors.verify(idx >= 0 && idx < totalBlocks());
			markAsRead(idx);
			int offset = permutation.map(idx);
			Errors.verify(offset >= 0 && offset < totalBlocks());
			
			// do a lookup to check whether the block we want isn't being cached out concurrently
			BlockDataItem bdi = getCachedOut(offset);
			if(bdi != null) 
			{ // we found the block in the process of being cached-out, let's pass it off as a sop
				Operation op = null;
				sop = new ScheduledOperation(op);
				sop.onSuccess(bdi.getEncryption());
			}
			else 
			{ 
				sop = scheduleDownload(reqId, offset); Errors.verify(sop != null); 
			}
			
			if(cacheIn == true) { cacheInBuffer.add(offset, sop); }
			
			return sop;
		}
		
		private int nextRandomDummyIndex() 
		{
			Errors.verify(dummyBlocks > 0 && dummyBlocksReadCount < dummyBlocks);
			int ret = -1;
			do { ret = realBlocks + rng.nextInt(dummyBlocks);	}
			while(blocksRead.get(ret) == true);
			return ret;
		}
		
		private int nextRandomRealIndex() 
		{
			Errors.verify(realBlocks > 0 && realBlocksReadCount < realBlocks);
			int ret = -1;
			do { ret = rng.nextInt(realBlocks);	}
			while(blocksRead.get(ret) == true);
			return ret;
		}
		
		private ScheduledOperation downloadNextDummy(long reqId) 
		{
			// get index of next random dummy block
			int nextDummyIdx = nextRandomDummyIndex();
			dummyBlocksReadCount++;
			
			if(shufflingRemainingBlocksToRead > 0) { shufflingRemainingBlocksToRead--; }
			
			ScheduledOperation sop = read(reqId, nextDummyIdx, false);
			
			Errors.verify(blocksRead.cardinality() == realBlocksReadCount + dummyBlocksReadCount);
			
			return sop;
		}
		
		private ScheduledOperation cacheInNextUnread(long reqId) 
		{
			Errors.verify(isMarkedForShuffling() == true);
			if(shufflingRemainingBlocksToRead == 0) { return null; } // we've read all the blocks we need to
			
			int nextIdx = blocksRead.nextClearBit(0); // get index of next unread block
			Errors.verify(nextIdx >= 0 && nextIdx < totalBlocks());
			
			return cacheInByIndex(reqId, nextIdx);
		}

		private ScheduledOperation cacheInByIndex(long reqId, int idx) 
		{
			Errors.verify(shufflingRemainingBlocksToRead == ShufflingNotScheduled || realBlocks - realBlocksReadCount <= shufflingRemainingBlocksToRead);
			
			Errors.verify(idx >= 0 && idx < totalBlocks());
			if(shufflingRemainingBlocksToRead > 0) { shufflingRemainingBlocksToRead--; }
			
			boolean isReal = isRealBlock(idx); // cache in only if the block is real
			
			if(isReal == true) { realBlocksReadCount++; }
			else { dummyBlocksReadCount++; }
			
			
			ScheduledOperation sop = read(reqId, idx, isReal);
			
			
			Errors.verify(blocksRead.cardinality() == realBlocksReadCount + dummyBlocksReadCount);
			
			return sop;
		}

		private boolean isRealBlock(int idx) 
		{
			Errors.verify(idx >= 0 && idx < totalBlocks());
			return (idx < realBlocks);
		}

		private synchronized List<BlockDataItem> getAndRemoveEarlyCacheIns() 
		{
			Errors.verify(blocksRead.nextClearBit(0) >= realBlocks || realBlocksReadCount < realBlocks);
			return cacheInBuffer.drainAll();
		}

		private synchronized Map<Integer, Integer> shuffleAndSetupRewrite(List<BlockDataItem> shufflingBuffer) 
		{
			// take at most totalBlocks()/2 real blocks from buffer, and totalBlocks() in total
			List<BlockDataItem> blocksTaken = new ArrayList<BlockDataItem>();
			
			int size = totalBlocks();
			int maxRealBlocks = size/2;
			
			Errors.verify(shufflingBuffer.size() >= size);
			
			// clear things
			clear();
			realBlocks = 0; dummyBlocks = 0;
			blocksRead.clear();
			permutation = new SimpleIntegerPermutation(size, rng);
			empty = false;
			
			// first take as many real blocks as possible
			for(BlockDataItem bdi : shufflingBuffer)
			{
				if(realBlocks == maxRealBlocks) { break; }
				
				ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader(); 
				boolean isDummy = (header.getBlockId() < 0);
				if(isDummy == false) { realBlocks++; blocksTaken.add(bdi); }
			}
			// now take dummy blocks to fill up
			for(BlockDataItem bdi : shufflingBuffer)
			{
				if(totalBlocks() == size) { break; }
				
				ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader();
				boolean isDummy = (header.getBlockId() < 0);
			
				if(isDummy == true)	{ dummyBlocks++; blocksTaken.add(bdi); }
			}
			Errors.verify(totalBlocks() == size);
			
			shufflingBuffer.removeAll(blocksTaken);  // remove the blocks that have been taken
			
			Map<Integer, Integer> newBlockOffsetsMap = new HashMap<Integer, Integer>();
			
			int realBlockIdx = 0; int dummyBlockIdx = realBlocks;
			for(BlockDataItem bdi : blocksTaken)
			{
				ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader();
				int blockId = header.getBlockId();
				boolean isDummy = (blockId < 0);
				
				int offset = -1;
				
				if(isDummy == false) { offset = permutation.map(realBlockIdx++); }
				else { offset = permutation.map(dummyBlockIdx++); }
				
				Errors.verify(offset >= 0 && offset < totalBlocks() && realBlockIdx <= realBlocks);
				
				if(isDummy == false) { newBlockOffsetsMap.put(blockId, offset); } // fill in the map
				
				// add to cache out buffer
				cacheOut(offset, bdi);
			}
			
			return newBlockOffsetsMap;
		}
		
		private synchronized void setupWithAllDummies(Random rng) 
		{
			// fill with all dummies (logically only)
			int size = totalBlocks();
			
			// clear things
			clear();
			realBlocks = 0; dummyBlocks = 0;
			blocksRead.clear();
			permutation = new SimpleIntegerPermutation(size, rng);
			empty = false;
			dummyInit = true;
			
			realBlocks = 0;
			dummyBlocks = size;
			Errors.verify(realBlocks + dummyBlocks == size);
		}

		private void cacheOut(int offset, BlockDataItem bdi) 
		{
			Errors.verify(offset >= 0 && offset < totalBlocks());
		
			cacheOutBuffer.addPrepared(offset, bdi);
		}
		
		private ScheduledOperation scheduleUpload(long reqId, int offset, BlockDataItem bdi) 
		{
			Errors.verify(offset >= 0 && offset < totalBlocks());
			UploadOperation upload = new UploadOperation(reqId, getKey(offset), bdi.getEncryption());
			ScheduledOperation sop = s.uploadObject(upload);
			
			return sop;
		}

		// when calling for a cache-out, the cache-out goes from prepared, to pending.
		// when all cache-outs are pending, prepared is empty;
		// when a cache-out completes it is removed from pending
		// eventually all cache-outs are removed and pending is empty
		
		public synchronized Entry<ScheduledOperation, Integer> cacheOutNextBlock(long reqId) 
		{
			Entry<Integer, BlockDataItem> entry = cacheOutBuffer.nextCacheOut();
			
			int offset = entry.getKey();
			BlockDataItem bdi = entry.getValue();
			
			ScheduledOperation sop = scheduleUpload(reqId, offset, bdi);
			
			return new AbstractMap.SimpleEntry<ScheduledOperation, Integer>(sop, offset);
		}
		
		public synchronized int remainingCacheOuts() { return cacheOutBuffer.getPreparedSize(); }
		
		public synchronized void removeCacheOut(int offset)
		{
			cacheOutBuffer.removePending(offset);
		}
		
		/*** both prepared and pending cache-outs are considered in progress ***/
		public synchronized int inProgressCacheOuts()  
		{ return cacheOutBuffer.getPreparedSize() + cacheOutBuffer.getPendingSize(); }

		public synchronized boolean dummyInit() { return dummyInit ; }

		public void save(ObjectOutputStream os) throws Exception
		{
			os.writeInt(idx);
			
			os.writeInt(pref); // -d-
			
			os.writeInt(realBlocks);
			os.writeInt(dummyBlocks);
			os.writeInt(realBlocksReadCount);
			os.writeInt(dummyBlocksReadCount);
			
			os.writeBoolean(empty);
			os.writeBoolean(dummyInit);
			
			os.writeBoolean(markedForShuffling);
			
			os.writeInt(shufflingRemainingBlocksToRead);
			
			boolean permNotNull = permutation != null;
			os.writeBoolean(permNotNull);
			if(permNotNull == true) { permutation.save(os); }
			
			os.writeObject(blocksRead);
			
			cacheInBuffer.save(os);
			cacheOutBuffer.save(os);
		}
		
		
		public Level(int partIdx, ObjectInputStream is, Random r)  throws Exception
		{
			rng = r;
			
			partition = partIdx;
			
			idx = is.readInt();
			
			pref = is.readInt(); // -d-
			
			realBlocks = is.readInt();
			dummyBlocks = is.readInt();
			realBlocksReadCount = is.readInt();
			dummyBlocksReadCount = is.readInt();
			
			empty = is.readBoolean();
			dummyInit = is.readBoolean();
			
			markedForShuffling = is.readBoolean();
			
			shufflingRemainingBlocksToRead = is.readInt();
			
			// load permutation
			boolean permNotNull = is.readBoolean();
			permutation = null;
			if(permNotNull == true)
			{
				permutation = new SimpleIntegerPermutation(1, new Random());
				permutation.load(is);
			}
			
			
			blocksRead = (BitSet)is.readObject();
			
			cacheInBuffer = new CacheInBuffer(is);
			cacheOutBuffer = new CacheOutBuffer(is);
		}
	}
	
	protected Random rng = null;
	protected ExternalStorageInterface s = null;
	
	protected int idx = 0;
	protected int size = 0;
	protected Level[] levels = null;
	

	protected AtomicBoolean shuffling = new AtomicBoolean(false);
	
	protected ReadWriteLock lock = new ReentrantReadWriteLock();
	
	public Partition(ExternalStorageInterface esi, int i, int numLevels, int topLevelCapacity, Random r)
	{
		rng = r;
		s = esi;
		idx = i;
		
		Errors.verify(numLevels > 0 && numLevels < 32);
		
		levels = new Level[numLevels];
		for(int j=0; j<numLevels-1; j++) { levels[j] = new Level(idx, j, (int)(1 << j), rng); }
		levels[numLevels-1] = new Level(idx, numLevels-1, topLevelCapacity, rng);
		
		size = 0;
		for(int j=0; j<numLevels; j++) { size += levels[j].getSize(); }
	}
	
	public int getIdx() { return idx; }
	

	public int getSize() { return size; }
	

	public boolean atomicSetShuffling()  { return shuffling.compareAndSet(false, true); }

	public void atomicResetShuffling() 
	{
		boolean success = shuffling.compareAndSet(true, false);
		Errors.verify(success == true);
	}

	public Level getLevel(int levelIdx) 
	{
		if(levelIdx < 0) { return null; }
		
		Errors.verify(levelIdx >= 0 && levelIdx < levels.length);
		return levels[levelIdx];
	}

	public Level[] getLevels() { return levels; }
	
	private int maxSizeFromCounter(int c)
	{
		int ret = 0; List<Level> list = levelsFromCounter(c);
		for(Level level : list) { ret += level.getSize(); }
		return ret;
	}
	
	private List<Level> levelsFromCounter(int c)
	{
		List<Level> ret = new ArrayList<Level>();
		for(int i=0; i<levels.length; i++)
		{
			boolean bitSet = (c & (0x1 << i)) != 0;
			if(bitSet == true) { ret.add(levels[i]); }
		}
		return ret;
	}
	
	private int getCounter()
	{
		int ret = 0;
		for(int i=0; i<levels.length; i++)
		{
			boolean bitSet = !levels[i].isEmpty();
			if(bitSet == true) { ret += (1 << i); }
		}
		return ret;
	}
	
	private int nonEmptyConsecutiveLevelsCount(int c)
	{
		for (int i = 0; i < 32; i++) { if ((c & (1 << i)) == 0) { return i; } }
		return -1;
	}
	
	// done *exactly* as in ObliviStore C# implementation
	public void getLevelsForReshuffling(int jobSize, List<Level> levelsToShuffle, List<Level> shuffleToLevels)
	{
		int initialCounter = getCounter();
		int c = initialCounter;
		int reshuffleLevelCount = 0;
		for (int i = 0; i < jobSize; i++)
		{
			reshuffleLevelCount = Math.max(reshuffleLevelCount, nonEmptyConsecutiveLevelsCount(c));
			c = (((c + 1) % (1 << (levels.length - 1))) | (1 << (levels.length - 1)));
		}

		int sourceMask = 0; int destinationMask = 0;
		if(reshuffleLevelCount > 0) 
		{
			int t = 0xFFFFFFFF;
			int m = (t >>> (32 - reshuffleLevelCount));
			sourceMask = (int)(initialCounter & m);
		}

		{
			int t = 0xFFFFFFFF;
			int m = (t >>> (32 - reshuffleLevelCount - 1));
			destinationMask = (int)(c & m);
		}
		levelsToShuffle.addAll(levelsFromCounter(sourceMask));
		shuffleToLevels.addAll(levelsFromCounter(destinationMask));
	}

	public void lock(boolean write) 
	{
		if(write == true) { lock.writeLock().lock(); }
		else { lock.readLock().lock(); }
	}

	public void unlock(boolean write) 
	{
		if(write == true) { lock.writeLock().unlock(); }
		else { lock.readLock().unlock(); }
	}

	public Map<Integer, Map.Entry<Integer, Integer>> localShuffle(Random rng, List<Level> levelsToShuffle, List<Level> shuffleToLevels, List<BlockDataItem> fromEvictionCache,
			Semaphore earlyCacheInsSemaphore, int sizeOfShuffleToLevels, BlockDataItem dummyBlock) 
	{
		List<BlockDataItem> shufflingBuffer = new ArrayList<BlockDataItem>();
		
		// Fetch from storage cache all cached-in blocks for levels marked for shuffling
		// 		for each cache-in that is an early cache-in, increment the early cache-in semaphore
		for(Partition.Level level : levelsToShuffle)
		{
			Errors.verify(level == levels[level.idx]); // make sure this is a level that belongs to that partition
			Errors.verify(level.isEmpty() == false && level.isMarkedForShuffling() == true && level.unreadBlocksForShuffling() == 0);
			
			List<BlockDataItem> earlyCacheIns = level.getAndRemoveEarlyCacheIns();
			shufflingBuffer.addAll(earlyCacheIns);
			
			earlyCacheInsSemaphore.release(earlyCacheIns.size());
			
			earlyCacheIns.clear();
			
			// unmark level for shuffling (and clear it)
			level.clear();
		}


		int maxRealBlocks = sizeOfShuffleToLevels / 2;
		for(BlockDataItem bdi : shufflingBuffer)
		{
			ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader(); 
			boolean isReal = (header.getBlockId() >= 0);
			Errors.verify(isReal == true, "Error: somehow we early-cached-in a dummy block!!");
		}
		int realBlocksCachedIn = shufflingBuffer.size();
		
		// add the ones from the eviction cache
		// idea: only add the non-dummy ones, since we'll pad with dummies later if needed, anyways
		int realBlocksFromEvictionCache = 0;
		for(BlockDataItem bdi : fromEvictionCache)
		{
			ObliviStoreHeader header = (ObliviStoreHeader)bdi.getHeader(); 
			boolean isReal = (header.getBlockId() >= 0);
			if(isReal == true) { shufflingBuffer.add(bdi); realBlocksFromEvictionCache++; }
		}
		
		int totalRealBlocks = realBlocksCachedIn + realBlocksFromEvictionCache;
		if(totalRealBlocks > maxRealBlocks)
		{
			Errors.error("[Partition (localShuffle)] somehow we are overflowing the partition; this should not happen! (capacity: " + maxRealBlocks + ", totalRealBlocks: " + totalRealBlocks + ")");
		}
		
		Errors.verify(shufflingBuffer.size() <= sizeOfShuffleToLevels);
		
		// pad to the jobSize
		while(shufflingBuffer.size() < sizeOfShuffleToLevels) { shufflingBuffer.add(dummyBlock.copy()); } // pad with dummies
		Collections.shuffle(shufflingBuffer, rng); // first shuffle the buffer
		
		Errors.verify(shufflingBuffer.size() == sizeOfShuffleToLevels);
		
		// distribute to the various levels
		Errors.verify(shuffleToLevels.isEmpty() == false);
		
		// store
		// store shuffled blocks into storage cache: for each level l, store exactly 2 2^l blocks from the shuffling buffer 
		// (at least half of which are dummy)
		
		Map<Integer, Map.Entry<Integer, Integer>> retMap = new HashMap<Integer, Map.Entry<Integer, Integer>>();
		for(Partition.Level level : shuffleToLevels)
		{
			Errors.verify(level == levels[level.idx]); // make sure this is a level that belongs to that partition
			
			Map<Integer, Integer> m = level.shuffleAndSetupRewrite(shufflingBuffer);
			for(int blockId : m.keySet())
			{
				//Errors.verify(retMap.containsKey(blockId) == false);
				int offset = m.get(blockId);
				
				retMap.put(blockId, new AbstractMap.SimpleEntry<Integer, Integer>(level.idx, offset));
			}
			// mark destination level as filled
			{ Errors.verify(level.isEmpty() == false); level.setFilled(); }	// note: this is redundant, level.isEmpty() returns false already			
		}
		Errors.verify(shufflingBuffer.isEmpty() == true); // make sure all blocks were taken to be rewritten somewhere
		
		return retMap;
	}

	public Map<Integer, Map.Entry<Integer, Integer>> initialize(Random rng, boolean fastInit, 
							List<Integer> blocks, List<BlockDataItem> shufflingBuffer, BlockDataItem dummyBlock) 
	{
		Map<Integer, Map.Entry<Integer, Integer>> retMap = new HashMap<Integer, Map.Entry<Integer, Integer>>();
		// exactly as in ObliviStore (except when fastInit is true, in which case, we additionally will also upload the blocks to storage)
		Level topLevel = levels[levels.length - 1];
		
		int realBlocks = blocks.size();
		int maxRealBlocks = 0;
		int initialLevelStatesMask = 0;
		//int msz = (1 << (topLevel.idx+1)) - 1;
		int msz = (1 << (topLevel.idx+1));
		do
		{
			initialLevelStatesMask = rng.nextInt(msz);
			initialLevelStatesMask |= (1 << topLevel.idx);
			maxRealBlocks = maxSizeFromCounter(initialLevelStatesMask)/2;
		}
		while(realBlocks > maxRealBlocks);
		
		if(fastInit == true)
		{	
			int totalBlocks = maxSizeFromCounter(initialLevelStatesMask);
			Errors.verify(shufflingBuffer != null); 
			while(shufflingBuffer.size() < totalBlocks) { shufflingBuffer.add(dummyBlock.copy()); }
		}
		
		for(Level level : levels)
		{
			boolean shouldBeFilled = ((level == topLevel) || ((initialLevelStatesMask & (1 << level.idx)) != 0));
		
			if(shouldBeFilled == true)
			{
				if(fastInit == true)
				{					
					Map<Integer, Integer> m = level.shuffleAndSetupRewrite(shufflingBuffer);
					
					for(int blockId : m.keySet())
					{
						Errors.verify(retMap.containsKey(blockId) == false);
						int offset = m.get(blockId);
						
						retMap.put(blockId, new AbstractMap.SimpleEntry<Integer, Integer>(level.idx, offset));
					}
				}
				else
				{
					Errors.verify(blocks != null && shufflingBuffer == null); 
					level.setupWithAllDummies(rng);
				}
			}
		}
		
		return retMap;
	}

	public void save(ObjectOutputStream os) throws Exception
	{
		os.writeInt(idx);
		os.writeInt(size);
		
		// save levels
		os.writeInt(levels.length);
		for(Level level : levels) { level.save(os); }
		
		Errors.verify(shuffling.get() == false);		
	}
	
	public Partition(ObjectInputStream is, ExternalStorageInterface s2, Random r) throws Exception
	{
		rng = r;
		
		s = s2;
		
		idx = is.readInt();
		size = is.readInt();
		
		int numLevels = is.readInt();
		levels = new Level[numLevels];
		for(int levelIdx = 0; levelIdx < levels.length; levelIdx++)	{ levels[levelIdx] = new Level(idx, is, rng); }
	}

}
