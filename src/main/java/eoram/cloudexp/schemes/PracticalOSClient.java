package eoram.cloudexp.schemes;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.data.BlockDataItem;
import eoram.cloudexp.data.CacheDataItem;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.IntegerSeededDataItem;
import eoram.cloudexp.data.PendingOpDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.WrappedListDataItem;
import eoram.cloudexp.data.encoding.DefaultHeader;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.implementation.AbstractClient;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.service.DeleteOperation;
import eoram.cloudexp.service.DownloadOperation;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.ListOperation;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.Errors;

/**
 *  Implements the PracticalOS scheme (see Goodrich, Michael T., et al. "Practical oblivious storage." CODASPY 2012.).
 *  <p><p>
 *  This implementation is based on the paper. 
 */
public class PracticalOSClient extends AbstractClient
{
	public static void saveBlocksMap(ObjectOutputStream os, Map<String, BlockDataItem> map) throws Exception
	{
		os.writeInt(map.size());
		for(String blockId : map.keySet())
		{
			os.writeObject(blockId);
			BlockDataItem bdi = map.get(blockId);
			Errors.verify(bdi != null);
			bdi.save(os);
		}
	}
	public static void loadBlocksMap(ObjectInputStream is, Map<String, BlockDataItem> map) throws Exception 
	{
		int size = is.readInt();
		while(size > 0)
		{
			String blockId = (String)is.readObject();
			BlockDataItem bdi = new BlockDataItem(is);
			map.put(blockId, bdi);
			size--;
		}
	}
	
	protected class LocalDictionary
	{
		protected Map<String, BlockDataItem> map = new HashMap<String, BlockDataItem>();
		
		//protected Set<String> keys = new HashSet<String>(); // keep track of existing keys (needed for fastinit == false)
		
		protected LocalDictionary() {}

		protected synchronized void add(String key, BlockDataItem bdi) { map.put(key, bdi); }
		protected synchronized BlockDataItem lookup(String key) { return map.get(key); }

		protected synchronized List<BlockDataItem> drainAll() 
		{
			List<BlockDataItem> ret = new ArrayList<BlockDataItem>(map.values());
			clear();
			return ret;
		}

		protected synchronized void clear() { map.clear(); }

		protected synchronized int getSize() { return map.size(); }

		protected void save(ObjectOutputStream os) throws Exception	{ PracticalOSClient.saveBlocksMap(os, map); }
		
		protected LocalDictionary(ObjectInputStream is) throws Exception { PracticalOSClient.loadBlocksMap(is, map); }
	}
	
	protected Lock lock = new ReentrantLock();
	
	protected LocalDictionary dict = new LocalDictionary();
	
	protected Condition shufflingCondition = null;
	protected AtomicInteger pendingRequests = null;
	
	protected long randomNonce = 0;
	protected AtomicInteger requestCount = new AtomicInteger(0);
	protected AtomicInteger nextDummyIdx = new AtomicInteger(0);
	protected int M = 0;
	
	protected AtomicInteger pendingDeletes = new AtomicInteger(0);
	
	protected static final int wrappingKeyByteSize = 8;
	
	protected static final int C = 2;
	protected static final int SortingRounds = 4; // as suggested in the paper
	
	protected boolean synchronous = false;
	//protected boolean synchronous = true;
	
	private static final int maxThreadCount = 128;
	protected ExecutorService executor = null;
	
	
	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{
		// load the dictionary
		dict = new LocalDictionary(is);
	
		randomNonce = is.readLong(); // load the random nonce
		
		requestCount = new AtomicInteger(is.readInt()); // load the request counter
		
		nextDummyIdx = new AtomicInteger(is.readInt()); // load the dummy idx counter
		
		M = is.readInt();
		
		synchronous = is.readBoolean();
	}
	
	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		// save the dictionary
		dict.save(os);
		
		os.writeLong(randomNonce); // save the random nonce
		
		os.writeInt(requestCount.get());
		
		os.writeInt(nextDummyIdx.get()); // save the dummy idx counter
		
		os.writeInt(M);
		
		os.writeBoolean(synchronous);
	}
	
	// override init and shutdown
	@Override
	protected void init(boolean reset)
	{
		Errors.verify(clientParams.maxBlocks <= Integer.MAX_VALUE, "Can't support that many blocks!");
		
		M = (int) Math.ceil(Math.pow(clientParams.maxBlocks, 1.0/(double)C));
		
		pendingRequests = new AtomicInteger(0);
		
		if(reset == true)
		{
			randomNonce = rng.nextLong();
			
			List<BlockDataItem> initItems = new ArrayList<BlockDataItem>();
		
			SessionState ss = SessionState.getInstance();
			boolean fastInit = ss.fastInit;
			
			Map<String, Request> fastInitMap = ss.fastInitMap;
			
			// for now, ensure fastInit is true
			Errors.verify(fastInit == true, "Initialization with fastInit == false is unsupported (for now)!");
			
			if(fastInit == true)
			{
				for(int blockId = 0; blockId < clientParams.maxBlocks; blockId++)
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
					BlockDataItem bdi = new BlockDataItem(new DefaultHeader(hexHashedKey("" + blockId)), di.getData());
					initItems.add(bdi);
				}
			}
			
			for(int i=0; i<M; i++) 
			{
				String reqKey = nextDummyReqKey();
				initItems.add(getDummyBlock(reqKey)); 
			}
			nextDummyIdx.set(0);
			
			// sort and upload items
			Map<String, BlockDataItem> wrappedItems = wrapRandomly(initItems, randomNonce);
			Pollable.waitForCompletion(scheduleUploads(Request.initReqId, wrappedItems));
		}
		
		executor = Executors.newFixedThreadPool(maxThreadCount);
	}
	
	@Override
	protected void shutdown()
	{
		lock.lock(); // ------------------------
		
		if(shufflingCondition != null)
		{ try { shufflingCondition.await(); } catch (InterruptedException e) { Errors.error(e); } }
		
		lock.unlock(); // ----------------------
		
		waitForPendingRequestsAndDeletes();
		
		executor.shutdown();
		
		try { Thread.sleep(100); } catch (InterruptedException e) { Errors.error(e); }
		
		log.append("[POSC] Waiting for all tasks to terminate...", Log.INFO);
		
		try { executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); } catch (InterruptedException e) { Errors.error(e); }
		
		log.append("[POSC] Done waiting, all tasks have completed.", Log.INFO);
	}

	@Override
	public boolean isSynchronous() { return synchronous; }

	@Override
	public String getName() { return "PracticalOSClient(" + synchronous + ")"; }
	
	protected String hexHashedKey(String key) { return cp.truncatedHexHashed(key, Header.getByteSize()); }
	
	
	protected BlockDataItem getDummyBlock(String reqKey)
	{
		return new BlockDataItem(new DefaultHeader(hexHashedKey(reqKey)), encodingUtils.getDummyBytes(clientParams.contentByteSize));
	}

	private String getWrappingKey(long nonce, String key)
	{
		CryptoProvider cp = CryptoProvider.getInstance();
		return cp.truncatedHexHashed(nonce + "||" + key, wrappingKeyByteSize);
	}
	
	private String getWrappingKey(String key) { return getWrappingKey(randomNonce, key); }
	
	private String nextDummyReqKey() { return "dummy" + nextDummyIdx.getAndIncrement(); }
	
	/*** Sort according to a random permutation order using wrapping with a random key ***/
	private Map<String, BlockDataItem> wrapRandomly(Collection<BlockDataItem> items, long nonce) 
	{
		Map<String, BlockDataItem> ret = new TreeMap<String, BlockDataItem>();
		
		for(BlockDataItem bdi : items)
		{
			String key = ((DefaultHeader)bdi.getHeader()).toString();
			ret.put(getWrappingKey(nonce, key), bdi);
		}

		return ret;
	}
	
	private Map<String, BlockDataItem> wrapRandomly(Collection<BlockDataItem> items)
	{
		long randomNonce = rng.nextLong();
		return wrapRandomly(items, randomNonce);
	}
	
	private List<String> getAllKeys(long reqId)
	{
		ScheduledOperation sop = s.listObjects(new ListOperation(reqId));
		
		sop.waitUntilReady();
		Errors.verify(sop.wasSuccessful() == true);
		
		WrappedListDataItem wldi = (WrappedListDataItem)sop.getDataItem();
		List<String> ret = wldi.getList();
		
		{ //- debug only
			if(ss.debug == true)
			{
				for(String s : ret)
				{
					Errors.verify(s.length() == 2 * wrappingKeyByteSize);
				}
			}
		}
		
		return ret;
	}
	
	private void shuffle(long reqId)
	{
		{ log.append("[POSC] Starting shuffling, message size: " + M, Log.TRACE); }
		
		final int messageSize = M;
		for(int i=0; i<SortingRounds; i++)
		{
			// first we get all the keys we need to shuffle
			List<String> keys = getAllKeys(reqId);
			
			for(int l=0; l<keys.size(); l+=messageSize)
			{
				Set<String> ks = new HashSet<String>();
				for(int j=0; j<messageSize; j++)
				{ if(l+j>= keys.size()) { break; } ks.add(keys.get(l+j)); }
				
				List<BlockDataItem> items = downloadAndDelete(reqId, ks);
				
				// sort and upload items
				Map<String, BlockDataItem> wrappedItems = wrapRandomly(items);
				Pollable.waitForCompletion(scheduleUploads(reqId, wrappedItems));
			}
			
			// make sure all pending deletes have completed
			waitForPendingRequestsAndDeletes();
		}
		
		// last pass, after updating the random nonce
		List<String> keys = getAllKeys(reqId);
		
		randomNonce = rng.nextLong(); // update the random nonce
		
		for(int l=0; l<keys.size(); l+=messageSize)
		{
			Set<String> ks = new HashSet<String>();
			for(int j=0; j<messageSize; j++)
			{ if(l+j>= keys.size()) { break; } ks.add(keys.get(l+j)); }
			
			List<BlockDataItem> items = downloadAndDelete(reqId, ks);
			
			// sort and upload items
			Map<String, BlockDataItem> wrappedItems = wrapRandomly(items, randomNonce);
			Pollable.waitForCompletion(scheduleUploads(reqId, wrappedItems));
		}
		
		// make sure all pending deletes have completed
		waitForPendingRequestsAndDeletes();
		
		// clear the local dictionary
		dict.clear();
		
		{ log.append("[POSC] Done with shuffling.", Log.TRACE); }
	}

	private void schedule(ScheduledRequest sreq)
	{
		boolean isPut = (sreq.getRequest().getType() == RequestType.PUT);
		
		long reqId = sreq.getRequest().getId();
		String reqKey = sreq.getRequest().getKey();
		String reqHashedKey = hexHashedKey(reqKey);
		
		{ log.append("[POSC (schedule)] req " + reqId + " reqKey " + reqKey + " -> " + reqHashedKey, Log.TRACE); }
		
		BlockDataItem ret = dict.lookup(reqHashedKey);

		if(ret != null)	{ reqHashedKey = hexHashedKey(nextDummyReqKey());	}
		
		BlockDataItem bdi = downloadAndDelete(reqId, reqHashedKey);
		Errors.verify(bdi != null);
		
		String hk = ((DefaultHeader)bdi.getHeader()).toString();
		Errors.verify(hk.equals(reqHashedKey));
		
		if(ret == null) 
		{
			ret = bdi;
			Errors.verify(ret != null);
		}
		
		if(isPut == true)
		{
			PutRequest put = (PutRequest)sreq.getRequest();
			ret.setPayload(put.getValue().getData());
		}
		
		dict.add(hk, bdi);
		Errors.verify(dict.getSize() <= M);
		
		if(isPut == true) { sreq.onSuccess(new EmptyDataItem()); }
		else { sreq.onSuccess(new SimpleDataItem(ret.getPayload())); }
		
		pendingRequests.decrementAndGet();
	}
	
	private void waitForPendingRequestsAndDeletes() 
	{
		while(pendingRequests.get() > 0 || pendingDeletes.get() > 0)
		{ try { Thread.sleep(10); } catch (InterruptedException e) { Errors.error(e); }	}
	}

	private ScheduledOperation scheduleUpload(long reqId, String objectKey,	DataItem di) 
	{
		return s.uploadObject(new UploadOperation(reqId, objectKey, di));
	}
	
	private Collection<? extends Pollable> scheduleUploads(long reqId, Map<String, BlockDataItem> wrappedItems) 
	{
		List<ScheduledOperation> ret = new ArrayList<ScheduledOperation>();
		for(String objectKey: wrappedItems.keySet())
		{
			BlockDataItem bdi = wrappedItems.get(objectKey);
			ScheduledOperation sop = scheduleUpload(reqId, objectKey, bdi.getEncryption());
			Errors.verify(sop != null); ret.add(sop);
		}
		return ret;
	}

	private ScheduledOperation scheduleDownload(long reqId, String objectKey)
	{
		return s.downloadObject(new DownloadOperation(reqId, objectKey));
	}
	
	private Collection<ScheduledOperation> scheduleDownloads(long reqId, Set<String> keys)
	{
		List<ScheduledOperation> ret = new ArrayList<ScheduledOperation>();
		for(String objectKey : keys)
		{
			ScheduledOperation sop = scheduleDownload(reqId, objectKey);
			Errors.verify(sop != null); ret.add(sop);
		}
		return ret;
	}
	
	private BlockDataItem downloadAndDelete(final long reqId, String reqKey) 
	{
		final String objectKey = getWrappingKey(reqKey);
		
		//{ log.append("[POSC (downloadAndDelete] req " + reqId + " downloading key: " + objectKey, Log.TRACE); }
		
		ScheduledOperation sop = scheduleDownload(reqId, objectKey);
		
		CacheDataItem cdi = new CacheDataItem(new PendingOpDataItem(sop));
		cdi.ensureOpened();
		
		pendingDeletes.incrementAndGet();
		//{ log.append("[POSC (downloadAndDelete)] waiting for deletion of key " + objectKey + " (req " + reqId + ") to finish.", Log.TRACE); }
	
		{
			ScheduledOperation sop2 = s.deleteObject(new DeleteOperation(reqId, objectKey));
			sop2.waitUntilReady();
			pendingDeletes.decrementAndGet();
		}
			
		//{ log.append("[POSC (downloadAndDelete)] Done waiting for deletion of key " + objectKey + " (req " + reqId + ") to finish.", Log.TRACE); }
			
		
		return cdi.getBlock();
	}
	
	private List<BlockDataItem> downloadAndDelete(final long reqId, final Set<String> keys) 
	{
		List<BlockDataItem> ret = new ArrayList<BlockDataItem>();
		
		Collection<ScheduledOperation> c = scheduleDownloads(reqId, keys);
		Pollable.waitForCompletion(c);
		
		for(int i = 0; i < keys.size(); i++) { pendingDeletes.incrementAndGet(); }
		
		{
			List<ScheduledOperation> pendingOps = new ArrayList<ScheduledOperation>();
			for(String objectKey : keys)
			{
				//{ log.append("[POSC (downloadAndDelete)] scheduling deletion of key " + objectKey + " (req " + reqId + ").", Log.TRACE); }
				ScheduledOperation sop2 = s.deleteObject(new DeleteOperation(reqId, objectKey));
				pendingOps.add(sop2);
			}
			Pollable.waitForCompletion(pendingOps);
			
			//{ log.append("[POSC (downloadAndDelete)] done waiting, all keys deleted (req " + reqId + ").", Log.TRACE); }
			
			for(int i = 0; i < keys.size(); i++) { pendingDeletes.decrementAndGet(); }
		}
		
		// decrypt and add all the blocks to 'ret'
		for(ScheduledOperation sop : c)
		{
			CacheDataItem cdi = new CacheDataItem(new PendingOpDataItem(sop));
			cdi.ensureOpened();
			
			ret.add(cdi.getBlock());
		}
		
		return ret;
	}
	
	private void _schedule(final ScheduledRequest sreq)
	{
		final long reqId = sreq.getRequest().getId();
		
		int reqCount = requestCount.incrementAndGet();
		
		Runnable r = new Runnable() 
		{ 
			@Override
			public void run() { schedule(sreq); }
		};

		
		lock.lock(); // ------------------------
		
		pendingRequests.incrementAndGet();
		Future<?> f = executor.submit(r);

		if((reqCount % M) == 0)
		{
			// make sure it is safe to continue, wait until all pending requests have completed (we can't do the shuffling concurrently)
			waitForPendingRequestsAndDeletes();
			
			List<BlockDataItem> dictItems = dict.drainAll();
			
			// upload all of the items in the dictionary before starting the shuffling
			Map<String, BlockDataItem> wrappedItems = wrapRandomly(dictItems);
			Pollable.waitForCompletion(scheduleUploads(reqId, wrappedItems));
		
			shuffle(reqId); // do the shuffling			
			nextDummyIdx.set(0); // reset the dummy idx counter
		}
		
		lock.unlock(); // ----------------------
		
		if(synchronous == true) { try { f.get(); } catch(Exception e) { Errors.error(e); } }
	}

	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		
		_schedule(sreq);		
		
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		
		_schedule(sreq);
		
		return sreq;
	}
	
	@Override
	public long peakByteSize() 
	{
		int entryByteSize = wrappingKeyByteSize + clientParams.contentByteSize + Header.getByteSize();
		return 2 * M * entryByteSize;
	}
}
