package eoram.cloudexp.schemes;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.IntegerSeededDataItem;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.implementation.AbstractClient;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.service.CopyOperation;
import eoram.cloudexp.service.DownloadOperation;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.Errors;

/**
 * Emulation of CCSW'11 GMOT paper: Oblivious RAM simulation with Efficient Worst-Case Access Overhead 
 * <p><p>
 * Note: this implementation only emulates the scheme, it will not pass correctness tests.
 * That said, the performance of this emulation should always be better than a fully functional implementation of the scheme.
 */
/// LayeredORAM aka SimHierarchicalClient
public class LayeredORAMClient extends AbstractClient 
{
	protected interface LogicalBuffer
	{
		public Set<String> getKeys();
		public Set<String> nextKeys(int requestCount, int c);
		
		public void clear();
		public void fill();
	}
	
	protected class SimpleBuffer implements LogicalBuffer
	{
		protected String name = null;
		protected int size = 0;
		protected boolean empty = true;
		
		protected SimpleBuffer(String n, int sz) { name = n; size = sz; }
	
		@Override
		public Set<String> getKeys() 
		{
			Set<String> ret = new TreeSet<String>();
			if(isEmpty() == true) { return ret; }
			
			for(int i=0; i<size; i++) { String key = name + "--" + i; ret.add(key); }
			return ret;
		}

		public boolean isEmpty() { return empty; }

		public void clear() { empty = true; }
		public void fill() { empty = false; }

		public Collection<? extends String> randomKeys(int count) 
		{
			Set<String> ret = new TreeSet<String>();
			while(ret.size() < Math.min(count, size)) 
			{ 
				String key = name + "--" + rng.nextInt(size); 
				ret.add(key); 
			}
			return ret;
		}

		@Override
		public Set<String> nextKeys(int requestCount, int c) 
		{
			int rc = requestCount;
			Set<String> ret = new TreeSet<String>();
			
			for(int i=0; i<c; i++)
			{ 
				int m = ((rc + i) % size);
				String key = name + "--" + m; ret.add(key); 
			}
			return ret;
		}
	}
	
	protected class CuckooTableSet
	{
		protected SimpleBuffer[] levels = null;
		protected CuckooTableSet(String name, int numLevels, int q)
		{
			Errors.verify(numLevels > 0 && q >= 1);
			levels = new SimpleBuffer[numLevels];
			
			for(int i=0; i<numLevels; i++)
			{
				int levelSize = q * (1 << i);
				levels[i] = new SimpleBuffer(name + "--level-" + i, levelSize);
			}
		}
		public Collection<? extends String> randomLocationsInLevel(int levelIdx) 
		{
			Errors.verify(levelIdx >= 0 && levelIdx < levels.length && levels[levelIdx].isEmpty() == false);
			Set<String> ret = new HashSet<String>();
			ret.addAll(levels[levelIdx].randomKeys(2));
			return ret;
		}
		public LogicalBuffer getLevel(int levelIdx) { return levels[levelIdx]; }
		public boolean isEmpty(int levelIdx) { return levels[levelIdx].isEmpty(); }
		public void clear(int levelIdx) { levels[levelIdx].clear(); }
	}
	
	protected class Workspace
	{
		protected CuckooTableSet tableSet = null;
		protected SimpleBuffer[] stashes = null;
		protected List<String> D = new ArrayList<String>(); // D as in the paper
		
		protected Workspace(int numLevels, int q)
		{
			tableSet = new CuckooTableSet("workspace-tableset", numLevels, q);
			stashes = new SimpleBuffer[numLevels];
			for(int i=0; i<numLevels; i++) { stashes[i] = new SimpleBuffer("workspace-stash--" + i, stashSize); }
		}

		public LogicalBuffer getLevel(int levelIdx) { return tableSet.getLevel(levelIdx); }

		public void clearList() { D.clear(); }

		public Set<String> getLastItems(int tsize) 
		{
			Set<String> ret = new HashSet<String>();
			Errors.verify(D.size() >= tsize);
			
			for(int i=D.size()-1; i>=0; i--)
			{
				if(ret.size() == tsize) { break; }
				ret.add(D.get(i));
			}
			Errors.verify(ret.size() == tsize);
			return ret;
		}

		public SimpleBuffer getStash(int levelIdx) { return stashes[levelIdx]; }

		public Set<String> addKeysToList(int size) 
		{
			int dsz = D.size();
			for(int idx=dsz; idx<dsz+size; idx++) { D.add("workspace-D--" + idx); }
			return getLastItems(size);
		}
	}
	
	protected CuckooTableSet currTableSet = null; // T_i
	protected CuckooTableSet prevTableSet = null; // T_i'
	
	protected SimpleBuffer currCache = null;
	protected SimpleBuffer prevCache = null;
	protected SimpleBuffer stash = null;
	
	protected Workspace workspace = null;
	
	protected int stashSize = 0;
	
	protected int epochSize = 0; // aka q = O(log(n))
	protected int numLevels = 0;
	protected int lastLevelIdx = 0;
	
	protected int requestCount = 0;
	protected TreeSet<Integer> I = null;
	
	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		// TODO Auto-generated method stub
		; // TODO
	}

	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		// TODO Auto-generated method stub
		; // TODO
	}
	
	// override init and shutdown
	@Override
	protected void init(boolean reset)
	{
		numLevels = (int)Math.ceil(Math.log(clientParams.maxBlocks)/Math.log(2.0));
		Errors.verify(numLevels > 0 && numLevels < 32); lastLevelIdx = numLevels - 1;
		
		final int qMultFactor = 1;
		epochSize = qMultFactor * numLevels;
		
		requestCount = 0;
		I = new TreeSet<Integer>();
		
		final int cacheSize = epochSize;
		currCache = new SimpleBuffer("current-cache", cacheSize);
		prevCache = new SimpleBuffer("prev-cache", cacheSize);
		
		final int s = 2;
		stashSize = numLevels;
		stash = new SimpleBuffer("stash", 2 * s * stashSize); // only 'stash' is of size 2 s log n
		
		currTableSet = new CuckooTableSet("current-tableset", numLevels, epochSize);
		prevTableSet = new CuckooTableSet("previous-tableset", numLevels, epochSize);
		
		workspace = new Workspace(numLevels, epochSize);
	}
	
	@Override
	protected void shutdown()
	{
		; // TODO
	}

	@Override
	public boolean isSynchronous() { return false; }

	@Override
	public String getName() { return "SimHierarchical"; }
	
	private Collection<? extends Pollable> scheduleDownloads(long reqId, Set<String> keys)
	{
		List<ScheduledOperation> ret = new ArrayList<ScheduledOperation>();
		for(String objectKey : keys)
		{
			ScheduledOperation sop = s.downloadObject(new DownloadOperation(reqId, objectKey));
			Errors.verify(sop != null); ret.add(sop);
		}
		return ret;
	}
	
	private Collection<? extends Pollable> scanAll(long reqId, Collection<LogicalBuffer> c)
	{
		Set<String> keys = new HashSet<String>();
		for(LogicalBuffer lb : c) { keys.addAll(lb.getKeys()); }
		return scheduleDownloads(reqId, keys);
	}
	
	private Collection<? extends Pollable> scheduleUploads(long reqId, Set<String> keys)
	{
		List<ScheduledOperation> ret = new ArrayList<ScheduledOperation>();
		
		DataItem dis[] = new DataItem[keys.size()];
		int i = 0; int size = clientParams.contentByteSize + clientParams.randomPrefixByteSize+  Header.getByteSize();
		for(String objectKey : keys) { dis[i] = new IntegerSeededDataItem(rng.nextInt(), size); i++; }
		
		
		i=0;
		for(String objectKey : keys)
		{
			ScheduledOperation sop = s.uploadObject(new UploadOperation(reqId, objectKey, dis[i]));
			Errors.verify(sop != null); ret.add(sop); i++;
		}
		return ret;
	}
	
	private Collection<? extends Pollable> rewrite(long reqId, LogicalBuffer lb)
	{
		Set<LogicalBuffer> s = new HashSet<LogicalBuffer>(); s.add(lb);
		return rewriteAll(reqId, s);
	}
	
	private Collection<? extends Pollable> rewriteAll(long reqId, Collection<LogicalBuffer> c)
	{
		Set<String> keys = new HashSet<String>();
		for(LogicalBuffer lb : c) { lb.fill(); keys.addAll(lb.getKeys()); }
		return scheduleUploads(reqId, keys);
	}
	
	private void schedule(ScheduledRequest sreq)
	{
		long reqId = sreq.getRequest().getId();
		
		// scan all the locations in cache currCache, and prevCache, and Stash
		Set<LogicalBuffer> c = new HashSet<LogicalBuffer>();
		c.add(currCache); c.add(prevCache); c.add(stash);
		Pollable.waitForCompletion(scanAll(reqId, c)); // to ensure obliviousness, we must wait for pending ops to complete
		
		// for each level i, 1 \leq i \leq L, do:
		for(int levelIdx = 0; levelIdx < numLevels; levelIdx++)
		{
			Set<String> keys = new HashSet<String>();
			// if i != L and T_i' is not empty -> access two random locations in T_i'
			if(prevTableSet.isEmpty(levelIdx) == false)
			{
				keys.addAll(prevTableSet.randomLocationsInLevel(levelIdx));
				if(keys.isEmpty() == false) 
				{
					// to ensure obliviousness, we must wait for pending downloads to complete
					Pollable.waitForCompletion(scheduleDownloads(reqId, keys)); 
					keys.clear();
				}
			}
			
			// if T_i is not empty -> access two random locations in T_i
			if(currTableSet.isEmpty(levelIdx) == false)
			{
				keys.addAll(currTableSet.randomLocationsInLevel(levelIdx));
				if(keys.isEmpty() == false) 
				{
					// to ensure obliviousness, we must wait for pending downloads to complete
					Pollable.waitForCompletion(scheduleDownloads(reqId, keys));
					keys.clear();
				}
			}
		}
		
		Set<Pollable> pending = new HashSet<Pollable>();
		// rewrite currCache, adding or replacing data item x
		pending.addAll(rewrite(reqId, currCache));
		
		// if x	is found in stash S	remove x from S. Rewrite S
		pending.addAll(rewrite(reqId, stash));
		
		// wait for completion of all rewrites at the same time
		Pollable.waitForCompletion(pending);
	
		// for i \in I
		for(int levelIdx : I)
		{
			// make the next 2b accesses towards a rebuild of table T_i^W
			final int b = 1;
			readWriteRebuildAccesses(reqId, requestCount, levelIdx, b);
		}
		
		// requestCount++
		requestCount++;
		
		// if requestCount mod q == 0, then
		if((requestCount % epochSize) == 0)
		{
			// end of epoch
			
			// copy currCache to prevCache and append it to D
			copy(reqId, currCache, prevCache);
			append(reqId, currCache, workspace);
			
			// empty currCache
			currCache.clear();
			
			// for i \in sorted_decr_order(I)
			// reverse iteration
			for(Iterator<Integer> it = I.descendingIterator(); it.hasNext();)
			{
			    int levelIdx = it.next();
			    
			    int tsize = epochSize * (1 << levelIdx);
			    // if requestCount mod  2^{i-1}q == 0
			    if((requestCount % tsize) == 0)
			    {
			    	// if i == L
			    	if(levelIdx == lastLevelIdx)
			    	{
			    		// copy T_i^W to T_L
			    		copy(reqId, workspace.getLevel(levelIdx), currTableSet.getLevel(levelIdx));
			    	}
			    	// else if T_i and T_i' are both full or both empty
			    	else if(currTableSet.isEmpty(levelIdx) == prevTableSet.isEmpty(levelIdx)) 			    	
			    	{
			    		// empty T_i' and copy T_i^W to T_i
			    		prevTableSet.clear(levelIdx);
			    		copy(reqId, workspace.getLevel(levelIdx), currTableSet.getLevel(levelIdx));
			    	}
			    	else // else
			    	{
			    		// copy T_i^W to T_i'
			    		copy(reqId, workspace.getLevel(levelIdx), prevTableSet.getLevel(levelIdx));
			    	}
			    	// merge S_i^W and S
			    	merge(reqId, workspace.getStash(levelIdx), stash);
			    }
			}
		    // for each level i, 1 \leq i \leq L, do:
		    for(int levelIdx = 0; levelIdx < numLevels; levelIdx++)
			{
		    	int tsize = epochSize * (1 << levelIdx);
		    	// if requestCount mod  2^{i-1}q == 0
			    if((requestCount % tsize) == 0)
			    {
			    	// copy last 2^{i-1}q items from D to T_i^W
			    	Set<String> srcKeys = workspace.getLastItems(tsize);
			    	
			    	workspace.getLevel(levelIdx).fill();
			    	Set<String> destKeys = workspace.getLevel(levelIdx).getKeys();
			    	copyKeys(reqId, srcKeys, destKeys);
			    	
				    if(I.contains(levelIdx) == false) { I.add(levelIdx); }
				    if(levelIdx == lastLevelIdx)
				    {
				    	// empty D
				    	workspace.clearList();
				    }
			    }
			}
		}
	}

	private void merge(long reqId, SimpleBuffer firstStash, SimpleBuffer secondStash) 
	{
		boolean emptyFirstStash = firstStash.isEmpty();
		firstStash.fill();
		Set<String> keys = firstStash.getKeys();
		if(emptyFirstStash == true) { Pollable.waitForCompletion(scheduleUploads(reqId, keys)); }
		
		keys.addAll(secondStash.getKeys());
		Pollable.waitForCompletion(scheduleDownloads(reqId, keys));
		Pollable.waitForCompletion(scheduleUploads(reqId, keys));
	}

	private void append(long reqId, SimpleBuffer c, Workspace w) 
	{
		Set<String> srcKeys = c.getKeys();
		Set<String> destKeys = w.addKeysToList(srcKeys.size());
		copyKeys(reqId, srcKeys, destKeys);
	}

	private void copy(long reqId, LogicalBuffer srcLevel, LogicalBuffer destLevel) 
	{
		destLevel.fill();
		copyKeys(reqId, srcLevel.getKeys(), destLevel.getKeys());
	}

	private void copyKeys(long reqId, Set<String> srcKeys, Set<String> destKeys)
	{
		Errors.verify(srcKeys.size() == destKeys.size());
		
		List<ScheduledOperation> pendingCopies = new ArrayList<ScheduledOperation>();
		
		Iterator<String> srcIter = srcKeys.iterator();
		Iterator<String> destIter = destKeys.iterator();
		while(srcIter.hasNext())
		{
			String srcKey = srcIter.next();
			String destKey = destIter.next();
			
			ScheduledOperation sop = s.copyObject(new CopyOperation(reqId, srcKey, destKey));
			pendingCopies.add(sop);
		}
		Pollable.waitForCompletion(pendingCopies);
	}

	private void readWriteRebuildAccesses(long reqId, int requestCount2, int levelIdx, int b) 
	{
		Set<String> readKeys = currTableSet.getLevel(levelIdx).nextKeys(requestCount, 1);
		Set<String> writeKeys = workspace.getLevel(levelIdx).nextKeys(requestCount, 1);
		
		if(readKeys.size() == 0) { Errors.verify(writeKeys.size() == 0); return; }
		
		String first = readKeys.iterator().next(); int i = 0;
		while(readKeys.size() < b)
		{
			String newKey = first + "---" + i++;
			readKeys.add(newKey);
		}
		
		first = writeKeys.iterator().next(); i = 0;
		while(writeKeys.size() < b)
		{
			String newKey = first + "---" + i++;
			writeKeys.add(newKey);
		}
		
		Set<Pollable> p = new HashSet<Pollable>();
		p.addAll(scheduleUploads(reqId, readKeys));
		p.addAll(scheduleUploads(reqId, writeKeys));
		Pollable.waitForCompletion(p);
		
		Pollable.waitForCompletion(scheduleDownloads(reqId, readKeys));
		Pollable.waitForCompletion(scheduleUploads(reqId, writeKeys));
	}

	@Override
	public ScheduledRequest scheduleGet(GetRequest req)
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		
		schedule(sreq);
		
		sreq.onSuccess(new IntegerSeededDataItem(rng.nextInt(), clientParams.contentByteSize));
		
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		
		schedule(sreq);
		
		sreq.onSuccess(new EmptyDataItem());
		
		return sreq;
	}

	@Override
	public long peakByteSize() 
	{
		return 0;
	}
	
}
