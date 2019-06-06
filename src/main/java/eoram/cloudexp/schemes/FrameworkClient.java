package eoram.cloudexp.schemes;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.data.*;
import eoram.cloudexp.data.encoding.FrameworkHeader;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.implementation.AbstractClient;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.primitives.AbstractPositionMap;
import eoram.cloudexp.schemes.primitives.DummyFactory;
import eoram.cloudexp.schemes.primitives.EvictionCache;
import eoram.cloudexp.schemes.primitives.FrameworkSubORAM;
import eoram.cloudexp.schemes.primitives.FrameworkSubORAM.ReadRemoveWriteInfo;
import eoram.cloudexp.service.*;
import eoram.cloudexp.service.Request.RequestType;
import eoram.cloudexp.threading.Job;
import eoram.cloudexp.threading.ThreadedJobsQueue;
import eoram.cloudexp.threading.ThreadedJobsQueue.SchedulingPriority;
import eoram.cloudexp.utils.Errors;

/**
 * Implements the main logic of the (CURIOUS) Framework Client.
 * <p><p>
 * To implement a CURIOUS based scheme, derive the new class from this, specifying the proper subORAM type as parameter.
 * <p>
 * @param <SubORAMType>
 */
public abstract class FrameworkClient<SubORAMType extends FrameworkSubORAM> extends AbstractClient implements DummyFactory
{
	protected enum LogicalPosition { InCache, InTransit, OnStorage };
	
	protected class Position
	{
		protected static final int InvalidPosition = -1;
		
		protected LogicalPosition logicalPos = null;
		protected int subORAMIdx = InvalidPosition;
		protected int withinPos = InvalidPosition;
		
		private Position(LogicalPosition l, int idx, int pos) { subORAMIdx = idx; withinPos = pos; logicalPos = l; }
		protected Position(LogicalPosition l) { this(l, InvalidPosition, InvalidPosition); }
		protected Position(int idx, int pos) { this(LogicalPosition.OnStorage, idx, pos); }
		
		protected void save(ObjectOutputStream os) throws Exception { os.writeInt(subORAMIdx); os.writeInt(withinPos); os.writeObject(logicalPos); }
		protected Position(ObjectInputStream is) throws Exception { subORAMIdx = is.readInt(); withinPos = is.readInt(); logicalPos = (LogicalPosition)is.readObject(); }
		protected LogicalPosition getLogical() { return logicalPos; }
		protected void setLogical(LogicalPosition l) { logicalPos = l; }
		
		protected Position copy() { return new Position(logicalPos, subORAMIdx, withinPos); }
		
		@Override
		public String toString() { return "(" + logicalPos.toString() + ", " + subORAMIdx + ", " + withinPos + ")"; }
		
		@Override
		public boolean equals(Object otherObj)
		{
			if(otherObj == this) { return true; }
			if(otherObj == null) { return false; }
			if(!(otherObj instanceof FrameworkClient.Position)) { return false; }
						
			Position otherPos = (Position)otherObj;
			
			if(logicalPos != otherPos.logicalPos) { return false; }
			if(subORAMIdx != otherPos.subORAMIdx) { return false; }
			if(withinPos != otherPos.withinPos) { return false; }
			
			return true;
		}
	}
	
	protected class PositionMap extends AbstractPositionMap<Position>
	{
		public PositionMap() { clear(); }
		
		public PositionMap(ObjectInputStream is) throws Exception
		{
			int mapSize = is.readInt(); // then the size
			for(int i=0; i < mapSize; i++)
			{
				String key = (String)is.readObject();
				Position pos = new Position(is);
				map.put(key, pos);
			}
		}
		
		public void save(ObjectOutputStream os) throws Exception
		{
			os.writeInt(getSize()); // map size
			for(String key : map.keySet())
			{
				Position pos = map.get(key); Errors.verify(pos != null);
				
				os.writeObject(key); // key
				pos.save(os); // save pos
			}
		}
	}
	
	protected class Task
	{
		protected ScheduledRequest sreq;				// the associated scheduled req
		
		protected String key = null;					// the block key
		protected String reqKey = null;					// the block hex hashed key
		protected String downloadReqKey = null;			// the hex hashed key of the download
		
		protected boolean isPut = false;				// whether the req is a put (or a get)
		protected long reqId = -1;						// the request id
		
		protected LogicalPosition logicalPos = null;	// the logical position of the block
		
		protected Position posToRead = null;			// the position to read
		
		protected BlockDataItem fromCache = null;		// the block from the cache
		
		protected BlockMoveListener listener = null;	// the listener (null for a put)
		
		protected Set<BlockDataItem> toRewrite = null;	// the evicted blocks to rewrite
		
		protected BlockDataItem toLog = null; 			// the block to write to log
		protected int logBlockIdx = -1;					// the log block idx 
		
		protected int versionId = -1;					// the versionId

		protected boolean completed = false;
	}
	
	protected class BlockMoveListener
	{
		public long reqId = -1;											// the request id
		protected String reqKey = null;									// the block reqKey
		protected int supVersionId = -1;								// the supremum of the version of id
		protected Position expectedPos = null;							// the expected position
		
		protected AtomicBoolean delivered = null;						// whether the item was delivered
		protected BlockingQueue<BlockDataItem> deliveryQueue = null;	// the delivery queue
	}
	
	protected boolean synchronous = false;
	//protected boolean synchronous = true;
	
	protected boolean reliable = false;
	//protected boolean reliable = true;
	
	protected boolean fastCompletion = false;
	//protected boolean fastCompletion = true;
	
	protected boolean dummyScheme = false;
	
	//protected boolean randomPrefix = false;
	protected boolean randomPrefix = true;
	
	protected boolean useExecutorToPoll = false;
	//protected boolean useExecutorToPoll = true;
	
	//protected boolean useSemaphoreForPendingOps = false;
	protected boolean useSemaphoreForPendingOps = true;
	
	
	protected boolean useSlots = true;
	//protected boolean useSlots = false;
	
	protected Lock localStateLock = new ReentrantLock(true);
	
	protected int logSize = 0;
	
	protected int numSubORAMs = 0;
	protected AtomicInteger requestCount = null;	
	protected PositionMap posMap = null;
	protected EvictionCache evictionCache = null;
	protected List<SubORAMType> subORAMs = new ArrayList<>();
	protected Queue<BlockDataItem> toLogBlockQueue = new ConcurrentLinkedQueue<>();
	
	protected Map<String, SortedMap<Integer, BlockMoveListener>> listeners = new HashMap<>();
	
	protected List<BlockingQueue<Task>> pendingTasks = new ArrayList<>();
	
	protected Semaphore pendingTasksSemaphore = null;
	protected int maxPendingTasks = 64;
	
	protected Semaphore pendingDownloadsSemaphore = null;
	protected Semaphore pendingUploadsSemaphore = null;

	protected static final String dummyKey = "dummy";
	protected static final String dummyHashedKey = CryptoProvider.getInstance().truncatedHexHashed(dummyKey, FrameworkHeader.encodedHexHashKeyByteSize);
	
	protected static final int maxThreadCount = 48;
	protected ExecutorService executor = null;
	
	protected ThreadedJobsQueue queue = null;
	
	private BlockMoveListener registerListener(long reqId, String reqKey, int versionId, Position expectedPos)
	{
		Errors.verify(reqKey != null && isDummy(reqKey) == false && versionId > 0);
		
		BlockMoveListener l = new BlockMoveListener();
		l.reqId = reqId;
		l.reqKey = reqKey;
		l.supVersionId = versionId;
		l.expectedPos = expectedPos;
		
		l.delivered = new AtomicBoolean(false);
		l.deliveryQueue = new ArrayBlockingQueue<>(1);
		
		localStateLock.lock(); // -------------------------------
		SortedMap<Integer, BlockMoveListener> map = listeners.get(l.reqKey);
		if(map == null)
		{
			listeners.put(l.reqKey, new TreeMap<Integer, BlockMoveListener>());
			map = listeners.get(l.reqKey);
		}
		
		BlockMoveListener other = map.put(l.supVersionId, l);
		Errors.verify(other == null); // there shouldn't be any other
		
		localStateLock.unlock(); // -------------------------------
		
		// -d-
		//{ log.append("[FC (registerListener)] Registered listener for block " + reqKey + " (supVersionId: " + versionId + ") for req " + l.reqId + ", expectedPos: " +  l.expectedPos + ")", Log.TRACE); }
		
		return l;
	}
	
	public interface FoundPosition<PType extends FrameworkClient.Position>
	{
		public String toString();
		public boolean matches(PType pos);
	}
	
	protected class DefaultFoundPosition implements FoundPosition<Position>
	{
		protected Position foundPos = null;
		protected DefaultFoundPosition(Position pos) { foundPos = pos; }
		
		@Override
		public boolean matches(Position pos) 
		{ 
			return foundPos.subORAMIdx == pos.subORAMIdx && foundPos.withinPos == pos.withinPos; //foundPos.equals(pos);
		}
		@Override
		public String toString() { return foundPos.toString(); }
	}
	
	private boolean findListeners(BlockDataItem bdi, String reqKey, int versionId, FoundPosition<Position> foundPos, boolean deliver, boolean onMove)
	{
		boolean ret = false;
		// see if we can find a listener for that block
		SortedMap<Integer, BlockMoveListener> map = listeners.get(reqKey);
		if(map != null)
		{
			SortedMap<Integer, BlockMoveListener> tailMap = map.tailMap(versionId+1);
			Iterator<Entry<Integer, BlockMoveListener>> iter = tailMap.entrySet().iterator();
			while(iter.hasNext() == true)
			{
				Entry<Integer, BlockMoveListener> entry = iter.next();
				int vid = entry.getKey(); BlockMoveListener l = entry.getValue();
				
				boolean foundWhereExpected = foundPos.matches(l.expectedPos);
				// -d-
				//{ log.append("[FC (findListeners)] Found listener for block " + reqKey + " (versionId: " + versionId + ") -> req " + l.reqId + " (vid: " + vid + ", expectedPos: " +  l.expectedPos + "), foundWhereExpected: " + foundWhereExpected + ", deliver: " + deliver, Log.TRACE); }
				
				if(foundWhereExpected == true)
				{
					ret = true;
	
					if(deliver == true)
					{
						Errors.verify(l.delivered.get() == false);
						l.delivered.set(true);
						boolean delivered = l.deliveryQueue.offer(bdi);
						Errors.verify(delivered == true);
						
						// -d-
						//{ log.append("[FC (findListeners)] Delivered block " + reqKey + " (versionId: " + versionId + ") to req " + l.reqId + " (vid: " + vid + ")", Log.TRACE); }
						
						if(onMove == false)
						{
							// based on the expected pos, do what needs to be done
							switch(l.expectedPos.logicalPos)
							{
							case InCache:
								Errors.error("Coding FAIL: shouldn't get hete!"); break;
							case InTransit:
								// don't do anything
								break;
							case OnStorage:
								// move to cache, if appropriate
								Position currentPos = posMap.getPos(reqKey);
								if(foundPos.matches(currentPos) == true)
								{
									boolean moved = tryCache(bdi, reqKey, versionId, new Position(LogicalPosition.InCache));
									// -d-
									//{ log.append("[FC (findListeners)] Listener for delivered block " + reqKey + " (versionId: " + versionId + ", req " + l.reqId + ") was completed by tryCache (moved: " + moved + ")", Log.TRACE); }
								}
								break;
							default:
								break;
							}
						}
						iter.remove(); // remove the listener, since we have delivered
					}
				}
			}
			
			if(map.isEmpty() == true) { listeners.remove(reqKey); }
		}
		if(ret == false)
		{
			;
			// -d-
			//{ log.append("[FC (findListeners)] No listeners for block " + reqKey + " (versionId: " + versionId + ")", Log.TRACE); }
		}
		
		return ret;
	}
	
	protected Entry<Boolean, Position> onFind(BlockDataItem bdi, FoundPosition<Position> foundPos, boolean deliver, boolean discard)
	{
		Errors.verify(bdi != null && foundPos != null);
		FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
		String reqKey = header.getKey(); int versionId = header.getVersionId();
		Errors.verify(isDummy(reqKey) == false && versionId >= 0);
		
		boolean keep = false;
		localStateLock.lock(); // -------------------------------
		
		Position currentPos = posMap.getPos(reqKey);
		if(foundPos.matches(currentPos) == true)
		{ 	
			keep = true;
		}
		
		// -d-
		//{ log.append("[FC (onFind)] Called onFind for block " + reqKey + " (versionId: " + versionId + "), currentPos: " + currentPos, Log.TRACE); }
		
		//if(keep == false || discard == true)
		{ // regardless of whether keep  is true
			boolean found = findListeners(bdi, reqKey, versionId, foundPos, deliver, false);
			if(found == true && deliver == false) { keep = true; }
		}
		
		localStateLock.unlock(); // -------------------------------
		
		// -d-
		//{ log.append("[FC (onFind)] Keep: " + keep + " for block " + reqKey + " (versionId: " + versionId + "), currentPos: " + currentPos, Log.TRACE); }
		
		return new AbstractMap.SimpleEntry<>(keep, currentPos);
		
	}
	
	protected boolean onMove(BlockDataItem bdi, FoundPosition<Position> foundPos, Position moveToPos)
	{
		Errors.verify(bdi != null && foundPos != null);
		boolean discard = moveToPos == null;
		
		FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
		String reqKey = header.getKey(); int versionId = header.getVersionId();
		Errors.verify(isDummy(reqKey) == false && versionId >= 0);
		
		boolean moved = false; 
		localStateLock.lock(); // -------------------------------
		
		{
			Position currentPos = posMap.getPos(reqKey);
			// -d-
			//log.append("[FC (onMove)] Attempting to move block " + reqKey + " (versionId: " + versionId + ") found at " + foundPos + " <currentPos: " + currentPos + ">, to " + (moveToPos == null ? "nowhere" : moveToPos), Log.TRACE);
		}
		
		findListeners(bdi, reqKey, versionId, foundPos, true, true);
		
		if(discard == false)
		{
			// attempt to move the block
			Position currentPos = posMap.getPos(reqKey);
			
			if(moveToPos.getLogical() == LogicalPosition.InCache) 
			{ 
				if(foundPos.matches(currentPos) == true)
				{ 
					moved = tryCache(bdi, reqKey, versionId, moveToPos);
				}
				else
				{
					// -d-
					//{ log.append("[FC (onMove)] Block " + reqKey + " (versionId: " + versionId + ") is not where it was found, discarding it.", Log.TRACE); }
				}
			}
			else 
			{
				if(foundPos.matches(currentPos) == true)
				{ 
					Position prevPos = posMap.setPos(reqKey, moveToPos);
					Errors.verify(prevPos.equals(currentPos));
					moved = true;
				
					// -d-
					//{ log.append("[FC (onMove)] Moved block " + reqKey + " (versionId: " + versionId + ") from " + prevPos + " to " + moveToPos + ", <found: " + foundPos + ">", Log.TRACE); }
				}
				else 
				{ // -d-
					//{ log.append("[FC (onMove)] Block " + reqKey + " (versionId: " + versionId + ") trying to move from " + currentPos + " to " + moveToPos + ", <found: " + foundPos + "> was discarded.", Log.TRACE); } 
				}
			}
		}
		else 
		{ 
			// -d-
			//{ log.append("[FC (onMove)] Block " + reqKey + " (versionId: " + versionId + ") was discarded.", Log.TRACE); } 
		}
		
		localStateLock.unlock(); // -------------------------------
		return moved;
	}

	protected boolean tryCache(BlockDataItem bdi, String reqKey, int versionId, Position moveToPos) 
	{
		boolean moved = evictionCache.put(reqKey, bdi); 
		if(moved == true) 
		{ 
			posMap.setPos(reqKey, moveToPos); 
			if(reliable == true){ toLogBlockQueue.add(bdi); } // add to the log queue if necessary
		}
		// -d-
		//{ log.append("[FC (onMove)] Tried to add block " + reqKey + " (versionId: " + versionId + ") to cache (success: " + moved + ").", Log.TRACE); }
		
		return moved;
	}
	
	
	private void prepareGetTask(final Task ret, final int versionId)
	{
		// figure out what needs to be done
		boolean dummyRead = false; 
		boolean inCache = false;
		boolean setupListener = false;
		
		Position currentPos = posMap.getPos(ret.reqKey); // lookup position of block
		Errors.verify(currentPos != null);
		
		switch(currentPos.getLogical())
		{
			case InCache: { dummyRead = true; inCache = true; setupListener = false; } break;
			case InTransit: { dummyRead = true; inCache = false; setupListener = true; } break;
			case OnStorage: { dummyRead = false; inCache = false; setupListener = true; } break;
			default: Errors.error("Coding FAIL: unknown logical position: " + currentPos.getLogical()); break;
		}
		
		ret.logicalPos = currentPos.getLogical();
	
		// set the position to read
		ret.posToRead = currentPos;
		if(dummyRead == true) { ret.posToRead = randomPosForRead(); }	
		
		ret.downloadReqKey = (dummyRead == true) ? dummyHashedKey : ret.reqKey;		

		// get it from cache if possible
		if(inCache == true) 
		{ 
			// -d-
			//{ log.append("[FC (prepareGetTask)] attempting to retrieve block with key " + ret.reqKey + " from the cache: " + currentPos, Log.TRACE); }
			ret.fromCache = evictionCache.lookup(ret.reqKey); 
			Errors.verify(ret.fromCache != null); 
		}
		
		// setup eviction cache hook if needed
		if(setupListener == true)
		{
			ret.listener = registerListener(ret.reqId, ret.reqKey, versionId, currentPos);
			Errors.verify(ret.listener != null);
		}
		
		// update the position of block if necessary
		if(inCache == false && dummyRead == false)
		{
			// we will query the block
			Position newPos = currentPos.copy(); newPos.setLogical(LogicalPosition.InTransit);
			Position oldPos = posMap.setPos(ret.reqKey, newPos);
			
			// -d-
			//{ log.append("[FC (prepareGetTask)] Changing logical pos of block " + ret.reqKey + ", from: " + oldPos + " to: " + newPos, Log.TRACE); }
		}
	}
	
	private BlockDataItem preparePutTask(final Task ret, final DataItem payload, final int versionId)
	{
		// for a put, we put the block immediately in the cache and then a dummy read follows
		FrameworkHeader h = new FrameworkHeader(ret.reqKey, versionId);
		BlockDataItem retBDI = new BlockDataItem(h, payload.getData());

		// do everything but put the block in cache, we will do this in 'prepareTask'
		
		// we do a dummy read
		ret.posToRead = randomPosForRead();	
		
		ret.logicalPos = null;
		
		return retBDI;
	}
	
	private Task prepareTask(final ScheduledRequest sreq, final int versionId)
	{
		final Request req = sreq.getRequest();
		
		Task ret = new Task();
		ret.sreq = sreq;
		ret.key = req.getKey();
		ret.versionId  = versionId;
		
		ret.completed = false;
		
		ret.isPut = (req.getType() == RequestType.PUT);
		final DataItem putPayload = (ret.isPut == true) ? ((PutRequest)req).getValue() : null;
		
		ret.reqId = req.getId();
		ret.reqKey = hexHashedKey(req.getKey());
		
		
		BlockDataItem putBDI = null;
		if(ret.isPut == true) { putBDI = preparePutTask(ret, putPayload, versionId); ret.downloadReqKey = dummyHashedKey; }
		else { prepareGetTask(ret, versionId); }
		
		Errors.verify(ret.posToRead.subORAMIdx < subORAMs.size());
		
		{ log.append("[FC (prepareTask)] task for key: " + ret.key +  " ("+ ret.reqKey + ", downloadReqKey: "  + ret.downloadReqKey +  ") will read pos: " + ret.posToRead, Log.TRACE); }
		
		
		if(putBDI != null)
		{
			Errors.verify(ret.isPut == true);
			
			// put the block in cache
			boolean added = evictionCache.put(ret.reqKey, putBDI);
			Errors.verify(added == true);
			
			// the block is not in the cache
			posMap.setPos(ret.reqKey, new Position(LogicalPosition.InCache));
		}
		
		// get blocks for eviction
		int evictCount = getMaxEvictCount(evictionCache.getOccupancy(), evictionCache.getCapacity());
		Set<BlockDataItem> toRewrite = evictionCache.evict(evictCount, ret.posToRead.subORAMIdx);		
		
		ret.toRewrite = toRewrite;		
		
		// update the pos for the blocks to rewrite
		for(BlockDataItem bdi : ret.toRewrite)
		{
			FrameworkHeader h = (FrameworkHeader)bdi.getHeader();
			String thisReqKey = h.getKey();
			
			int idx = ret.posToRead.subORAMIdx;
			
			Position newPos = new Position(idx, -h.getVersionId());
			// -d-
			//{ log.append("[FC (prepareTask)] will evict block with key " + thisReqKey + ", version: " + h.getVersionId() + " to pos: " + newPos, Log.TRACE); }
			
			Position foundPos = posMap.getPos(thisReqKey);
			Errors.verify(foundPos.getLogical() == LogicalPosition.InCache);
		
			onMove(bdi, new DefaultFoundPosition(foundPos), newPos);
		}
		
		
		// if reliability is enabled then we get the next block to log (could be a dummy one)
		if(reliable == true)
		{
			ret.logBlockIdx = versionId % logSize;
			ret.toLog = toLogBlockQueue.poll();
			if(ret.toLog == null) { ret.toLog = getDummyBlock(); }
			
			// add the block if it's a put
			if(putBDI != null) { toLogBlockQueue.add(putBDI); }
		}
		
		return ret;
	}
	
	/** override if needed **/
	protected int getMaxEvictCount(int occupancy, int capacity) 
	{
		return (int)Math.ceil((double)evictionCache.getOccupancy()/evictionCache.getCapacity());
	}

	private String hexHashedKey(String key) { return cp.truncatedHexHashed(key, FrameworkHeader.encodedHexHashKeyByteSize); }
	protected boolean isDummyBlock(String hashedKey) { return (hashedKey.equalsIgnoreCase(dummyHashedKey) == true); }
	public boolean isDummy(String key) { return isDummyBlock(key); } 
	protected BlockDataItem getDummyBlock() { return new BlockDataItem(new FrameworkHeader(dummyHashedKey, 0), encodingUtils.getDummyBytes(clientParams.contentByteSize)); }
	public BlockDataItem create() { return getDummyBlock(); }

	private Position randomPosForRead() 
	{
		int idx = rng.nextInt(subORAMs.size());
		return new Position(idx, subORAMs.get(idx).getDummyPos());
	}

	protected String getObjectKeyFromPos(int subORAMIdx, int withinPos) 
	{
		String str = "B-" + subORAMIdx  + "--" + withinPos;
		String prefix = "";
		if(randomPrefix == true) { prefix += cp.getHexHash(str.getBytes(Charset.forName("UTF-8"))).substring(0, 8) + "--"; }
		return prefix + str;
	}

	@Override
	public long peakByteSize() 
	{
		final double bitsPerByte = 8.0;
		
		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long cacheSize = evictionCache.getPeakOccupancy() * entryByteSize;
		
		int logMaxBlocks = (int)Math.ceil(Math.log(clientParams.maxBlocks)/Math.log(2.0));
		
		int subORAMSize = getSubORAMSize();
		
		int partIdxBits = (int)Math.ceil(Math.log(subORAMs.size())/Math.log(2.0));
		int offsetBits = (int)Math.ceil(Math.log(subORAMSize)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil((partIdxBits + offsetBits)/bitsPerByte);
		long posMapSize = posMap.getSize() * posMapEntrySize;
		
		long subORAMStateSize = (long)(subORAMs.size() * (getSubORAMStateSize()/bitsPerByte));
		
		return cacheSize + posMapSize + subORAMStateSize;
	}

	@Override
	protected void load(ObjectInputStream is) throws Exception 
	{		
		maxPendingTasks = is.readInt();
		
		synchronous = is.readBoolean();
		reliable = is.readBoolean();
		fastCompletion = is.readBoolean();
		dummyScheme = is.readBoolean();
		randomPrefix = is.readBoolean();
		useExecutorToPoll = is.readBoolean();
		useSemaphoreForPendingOps = is.readBoolean();
		
		logSize = is.readInt();
		int sz = is.readInt(); Errors.verify(sz == numSubORAMs);
		requestCount = new AtomicInteger(is.readInt());
		
		posMap = new PositionMap(is); // position map
		
		evictionCache = new EvictionCache(is, rng); // eviction cache
		
		// subORAMs
		Errors.verify(subORAMs.size() == 0);
		for(int idx = 0; idx < numSubORAMs; idx++)
		{
			SubORAMType o = loadSubORAM(is);
			subORAMs.add(o);
		}

		// toLogBlockQueue
		int toLogBlockQueueSize = is.readInt();
		for(int i=0; i<toLogBlockQueueSize; i++) { toLogBlockQueue.add(new BlockDataItem(is)); }
	}

	@Override
	protected void save(ObjectOutputStream os) throws Exception 
	{
		os.writeInt(maxPendingTasks);
		
		os.writeBoolean(synchronous);
		os.writeBoolean(reliable);
		os.writeBoolean(fastCompletion);
		os.writeBoolean(dummyScheme);
		os.writeBoolean(randomPrefix);
		os.writeBoolean(useExecutorToPoll);
		os.writeBoolean(useSemaphoreForPendingOps);

		os.writeInt(logSize);
		os.writeInt(numSubORAMs); Errors.verify(numSubORAMs == subORAMs.size());
		os.writeInt(requestCount.get());
		
		posMap.save(os); // position map
		
		evictionCache.save(os); // eviction cache
		
		// subORAMs
		for(SubORAMType o : subORAMs) { o.save(os); }

		// toLogBlockQueue
		os.writeInt(toLogBlockQueue.size());
		BlockDataItem[] tmp = toLogBlockQueue.toArray(new BlockDataItem[0]);
		for(BlockDataItem bdi : tmp) { bdi.save(os); }
		
		Errors.verify(listeners.isEmpty() == true);
		for(BlockingQueue<Task> q : pendingTasks)
		{
			Errors.verify(q.isEmpty() == true);
		}
	}
	
	public FrameworkClient(int m, int mpt, boolean sync, boolean reliab, boolean fc, boolean rndPrefix, boolean uetp, boolean usfpop)
	{
		numSubORAMs = m;
		Errors.verify(numSubORAMs > 0);

		// set the proper header
		Header.setCurrentHeader(new FrameworkHeader(dummyHashedKey, 0));
		
		maxPendingTasks = mpt;
		
		synchronous = sync;
		reliable = reliab;
		fastCompletion = fc;
		randomPrefix = rndPrefix;
		
		useExecutorToPoll = uetp;
		useSemaphoreForPendingOps = usfpop;
		
		{ log.append("[FC (constructor)] Setting up FC with " + m + " subORAMs, maxPendingTasks: " + maxPendingTasks + ", sync: " + synchronous + ", reliable: " + reliable + ", fastCompletion: " + fastCompletion + ", randomPrefix: " + randomPrefix + ", useExecutorToPoll: " + useExecutorToPoll + ", useSemaphoreForPendingOps: " + useSemaphoreForPendingOps, Log.INFO); }
	}
	
	public FrameworkClient(int m)
	{
		this(m, 64, false, false, false, false, false, true);
	}

	// subclasses must implement these
	protected abstract SubORAMType loadSubORAM(ObjectInputStream is) throws Exception;
	protected abstract SubORAMType initializeSubORAM(int idx, int capacity, boolean fastInit, Collection<BlockDataItem> blocks);
	
	// override init and shutdown
	@Override
	protected void init(boolean reset)
	{
		long n = clientParams.maxBlocks; int m = numSubORAMs;
		int expectedBlocksPerSubORAM = (int)Math.ceil((double)n / m);
		
		// log (circular buffer for reliability) size is  c * m * log_2 m
		final int c = 4;
		logSize = c * m * (int)Math.ceil(Math.log(m) / Math.log(2.0));
		if(logSize == 0) { logSize = c; }
		
		if(reset == true)
		{
			requestCount = new AtomicInteger(1); // initially 1 so versionId starts at 1
			
			double overflowBallsOS = ObliviStoreClient.calculateExtraCapacityFraction(m) * expectedBlocksPerSubORAM;
			final int capacityOS = expectedBlocksPerSubORAM + (int)Math.ceil(overflowBallsOS);

			final double acceptableOverflowProbExponent = -40; // 2^(-40) is fine
			final int trueCapacity = calculateSafeCapacity(n, m, acceptableOverflowProbExponent);
			
			//final int capacity = Math.min(trueCapacity, capacityOS);
			final int capacity = trueCapacity; // don't use OS' capacity, it doesn't work for small n/m values.
			
			{ log.append("[FC (init)] balls: " + n + ", bins: " + m + ", expected: " + expectedBlocksPerSubORAM + ", capacity: " + capacity + " (trueCapacity: " + trueCapacity + ", capacityOS: " + capacityOS + ")", Log.INFO); }
			
			SessionState ss = SessionState.getInstance();
			if(ss.fastInit == true)
			{ 
				Map<String, Request> fastInitMap = ss.fastInitMap;
				
				List<List<Integer>> blockIdsPerSubORAM = new ArrayList<>();
				for(int idx = 0; idx < numSubORAMs; idx++) { blockIdsPerSubORAM.add(new ArrayList<Integer>()); }
				Errors.verify(blockIdsPerSubORAM.size() == numSubORAMs);
				
				for(int blockId=0; blockId<n; blockId++)
				{
					int subORAM = rng.nextInt(m);
					blockIdsPerSubORAM.get(subORAM).add(blockId);
				}
				
				subORAMs.clear();
				Errors.verify(subORAMs.size() == 0);
				
				posMap = new PositionMap();
			
				int initVersionId = requestCount.get();
				int maxVersionId = 0;
				for(int idx = 0; idx < numSubORAMs; idx++)
				{
					List<BlockDataItem> toUploadBDI = new ArrayList<>();
					
					List<Integer> blockIds = blockIdsPerSubORAM.get(idx);
					for(int blockIdx : blockIds)
					{
						BlockDataItem bdi = null; String hashedKey = null;
						String blockKey = "" + blockIdx;
						hashedKey = hexHashedKey(blockKey); 
						
						int versionId = initVersionId;
						DataItem di = null;
						if(fastInitMap != null && fastInitMap.containsKey(blockKey)) // if we can, get it from fastInitMap
						{
							Request req = fastInitMap.get(blockKey);
							Errors.verify(req != null && req.getType() == RequestType.PUT);
							PutRequest put = (PutRequest)req;
							versionId = (int)put.getId();
							byte[] val = put.getValue().getData();
							if(val.length < clientParams.contentByteSize) { val = Arrays.copyOf(val, clientParams.contentByteSize); }
							di = new SimpleDataItem(val);
						}
						else { di = new IntegerSeededDataItem(blockIdx, clientParams.contentByteSize); }
						
						bdi = new BlockDataItem(new FrameworkHeader(hashedKey, versionId), di.getData());
						
						if(versionId > maxVersionId) { maxVersionId = versionId; }
						
						toUploadBDI.add(bdi);
					}
					
					Collections.shuffle(toUploadBDI, rng); // shuffle the blocks
					
					SubORAMType o = initializeSubORAM(idx, capacity, ss.fastInit, toUploadBDI);
					subORAMs.add(o);
					
					{ log.append("[FC (init)] Uploaded " + toUploadBDI.size() + " blocks to subORAM " + idx + " (max: " + capacity + ")", Log.INFO); }
				}
				
				if(initVersionId != maxVersionId) { maxVersionId++; }
				requestCount.set(maxVersionId); // set it so we get consistent ids even if we don't start at req 1.
			} 
			else
			{
				for(int idx = 0; idx < numSubORAMs; idx++)
				{
					SubORAMType o = initializeSubORAM(idx, capacity, ss.fastInit, null);
					subORAMs.add(o);
				}
			}
			
			Errors.verify(subORAMs.size() == numSubORAMs);
			
			evictionCache = new EvictionCache(rng, numSubORAMs);
		}
	
		while(pendingTasks.size() < numSubORAMs) { pendingTasks.add(new LinkedBlockingQueue<Task>()); } 
		
		int threadCount = Math.min(subORAMs.size(), maxThreadCount); // maxThreadCount;
		//executor = Executors.newFixedThreadPool(threadCount); //Executors.newCachedThreadPool(); 
		
		executor = Executors.newCachedThreadPool(); 
		
		//threadCount = Math.max(threadCount, maxPendingTasks);
		
		final int pendingSlackThreads = 8;
		queue = new ThreadedJobsQueue(threadCount + pendingSlackThreads) { };
		
		pendingTasksSemaphore = new Semaphore(maxPendingTasks, true);
		
	
		
		{ log.append("[FC (init)] Done initializing FC with " + m + " subORAMs, maxPendingTasks: " + maxPendingTasks + ", threadCount: " + threadCount, Log.INFO); }
		
		if(useSemaphoreForPendingOps == true)
		{ 
			SubORAMType o = subORAMs.get(0);
			int[] array = o.getReadWriteCounts(); Errors.verify(array.length == 4);
			int meanDownloadCount = array[0]; int maxDownloadCount = array[1];
			int meanUploadCount = array[2]; int maxUploadCount = array[3];
			
			int maxPendingDownloads = Math.max(maxPendingTasks * meanDownloadCount, maxDownloadCount + 8 * meanDownloadCount);
			pendingDownloadsSemaphore = new Semaphore(maxPendingDownloads, true);
			
			int maxPendingUploads = Math.max(maxPendingTasks * meanUploadCount, maxUploadCount + 8 * meanUploadCount);
			pendingUploadsSemaphore = new Semaphore(maxPendingUploads, true);
			
			log.append("[FC (init)] Using pending ops semaphores -- maxPendingDownloads: " + maxPendingDownloads + ", maxPendingUploads: " + maxPendingUploads, Log.INFO); 
		}
	}

	private int calculateSafeCapacity(long n, int m, final double log2AcceptableOverflowProbability)
	{	
		int expected = (int)Math.ceil((double)n/m);
		
		int minInc = Math.min(expected, 5);
		int ret = expected;
		
		double log2ProbOverflow = 0.0;
		do
		{
			ret += minInc;
			if(ret > n) { ret = (int)n; break; }
			
			int k = ret;
			//log2ProbOverflow = getLog2PoissonTailBound(n, m, k);
			log2ProbOverflow = Math.log(getNormalTailBound(n, m, k))/Math.log(2.0);
		}
		while(log2ProbOverflow > log2AcceptableOverflowProbability);
		
		return ret;
	}

	/** Poisson tail bound (works well when n/m is constant) **/
	private double getLog2PoissonTailBound(long n, int m, int k) 
	{
		double lambda = (double)n/m;
		double p1 = -lambda + Math.log(1.0 / (1.0 - lambda / (double)k));
		
		double p2 = 0.0;
		for(int i=1; i<=k; i++)	{ p2 += Math.log(lambda / i); }
		
		double logp = Math.log(m) + p1 + p2; // upper bound
		
		return (logp / Math.log(2.0));
	}
	
	/** Based on http://www.hindawi.com/journals/isrn/2013/412958/#B5
		and: "A Complete Proof of Universal Inequalities for the Distribution Function of the Binomial Law"
		(Also see http://math.stackexchange.com/questions/74760/bounds-on-normal-distribution)
		First bound Prob(B_1 > k), where B_1 is the number of balls in bin 1 using the above results; then use union bound.
		**/
	private double getNormalTailBound(long n, int m, int k) 
	{
		double p = 1.0 / m;
		
		double c = (double)k/n;
		double D = c * Math.log(c / p) + (1.0 - c) * Math.log((1.0 - c)/(1.0 - p));
		
		double t = Math.signum(k - n * p) * Math.sqrt(2 * n * D);
		
		double e = Math.exp(-(t * t) / 2.0);
		
		return m*(1.0 / (Math.sqrt(2 * Math.PI) * t)) * e;
	}
	
	@Override
	protected void shutdown()
	{
		log.append("[FC (shutdown)] Waiting for all tasks to terminate...", Log.INFO);
		while(pendingTasksSemaphore.hasQueuedThreads() || pendingTasksSemaphore.availablePermits() < maxPendingTasks)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { Errors.error(e); }
		}
		log.append("[FC (shutdown)] Done will all pending tasks.", Log.INFO);
		
		queue.shutdown();
		
		if(executor != null) 
		{
			executor.shutdown();			
			try { executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); } catch (InterruptedException e) { Errors.error(e); }
		}
		
		super.shutdown();
	}

	@Override
	public boolean isSynchronous() { return synchronous; }
	
	protected abstract String getNameOfSubORAM();
	protected abstract int getSubORAMSize(); // in number of addressable elements
	protected abstract int getSubORAMStateSize(); // in bits
	
	@Override
	public String getName() 
	{
		return "FrameworkClient<" + getNameOfSubORAM() + ">(" + numSubORAMs + ")";
	}
	
	protected Position lookupPosition(String reqKey)
	{
		localStateLock.lock(); //------------------------
		Position pos = posMap.getPos(reqKey);
		localStateLock.unlock(); //----------------------
		
		return pos;
	}
	protected Position updatePosition(String reqKey, Position newPos)
	{
		localStateLock.lock(); //------------------------
		Position oldPos = posMap.setPos(reqKey, newPos);
		localStateLock.unlock(); //----------------------
		
		return oldPos;
	}
	
	public class RewriteInfo
	{
		protected String reqKey = null;
		protected SubORAMType o = null;
		protected Position posToRead = null;
		
		protected Map<Integer, ScheduledOperation> downloads = null;
		protected Set<Integer> writeSet = null;
		protected Set<Integer> removeSet = null;
		protected Set<BlockDataItem> rewriteBlocks = null;
		
		protected Map<Integer, DataItem> rewriteMap = null;
		
		protected AtomicBoolean delivered = null;
		
	}
	
	protected abstract void prepareRewrite(RewriteInfo ri);
	
	private Callable<Void> processTask(final long reqId, Task task)
	{
		if(fastCompletion == true)
		{
			Errors.verify(task.completed == false);
			if(task.isPut == true) // fast completion of PUTs
			{
				completeTask(task, null);
				{ log.append("[FC (processTask)] Fast completion of (PUT) req " + task.reqId + " with key " + task.key + " (" + task.reqKey + ", versionId: " + task.versionId + ")", Log.TRACE);}
			}
			else if(task.fromCache != null) // fast completion of 'in cache' GETs
			{
				Errors.verify(task.isPut == false);
				BlockDataItem fastBDI = task.fromCache;
				completeTask(task, fastBDI);
				{ log.append("[FC (processTask)] Fast completion of (GET) req " + task.reqId + " with key " + task.key + " (" + task.reqKey + ", versionId: " + task.versionId + ") through cache", Log.TRACE);}
			}			
		}
		
		if(reliable == true)
		{
			// schedule the upload of the block to log
			String logBlockObjectKey = "log--B-" + task.logBlockIdx; 
			UploadOperation up = new UploadOperation(reqId, logBlockObjectKey, task.toLog.getEncryption());
			ScheduledOperation sop = s.uploadObject(up);
			Errors.verify(sop != null); // no need to wait for completion
		}
		
		Errors.verify(task != null && task.posToRead != null && task.posToRead.getLogical() == LogicalPosition.OnStorage);
		
		final int subORAMIdx = task.posToRead.subORAMIdx;
		int withinPos = task.posToRead.withinPos;
		
		final SubORAMType o = subORAMs.get(subORAMIdx);
		o.acquireOwnership(); // acquire ownership
		
		{ log.append("[FC (processTask)] started processing of req " + task.reqId + " working on subORAM " + subORAMIdx + " (downloadReqKey: " + task.downloadReqKey + ")", Log.TRACE);}
		
		String effectiveReqKey = task.downloadReqKey;
	
		localStateLock.lock(); //---------------------------
		if(isDummy(task.downloadReqKey) == false)
		{
			Position pos = posMap.getPos(task.downloadReqKey);
			
			// -d-
			//{ log.append("[FC (processTask)] req " + task.reqId + " acquired ownership of subORAM " + subORAMIdx + " (downloadReqKey: " + task.downloadReqKey + ") from pos: " + pos + ", withinPos: " + withinPos, Log.TRACE);}
			
			int dummyWithinPos = o.getDummyPos();
			
			Errors.verify(task.listener != null);
			boolean deliveredAlready = task.listener.delivered != null && task.listener.delivered.get() == true;
			boolean invalidPos = (o.isDummyPos(withinPos) == false) && (withinPos < 0 || withinPos == Position.InvalidPosition);
			boolean isEviction = invalidPos;
			
			boolean match = pos.subORAMIdx == subORAMIdx && pos.withinPos == withinPos && !invalidPos;
			
			////if(match == false)
			{
				// if it's already been delivered -> dummy
				if(deliveredAlready == true) { withinPos = dummyWithinPos; effectiveReqKey = dummyHashedKey; }
				// if it's a block that will eventually be evicted -> dummy
				else if(isEviction == true) { withinPos = dummyWithinPos; effectiveReqKey = dummyHashedKey; }
			}
			
			if(fastCompletion == true && task.completed == false && deliveredAlready == true) // fast completion of concurrent GETs
			{
				Errors.verify(task.isPut == false);
				BlockDataItem fastBDI = null;
				try { fastBDI = task.listener.deliveryQueue.take(); } catch (InterruptedException e) { Errors.error(e); }
				completeTask(task, fastBDI);
				
				{ log.append("[FC (processTask)] Fast completion of (GET) req " + task.reqId + " with key " + task.key + " (" + task.reqKey + ", versionId: " + task.versionId + ") through delivery", Log.TRACE);}
			}
			
			// -d-
			//{ log.append("[FC (processTask)] req " + task.reqId + " block with reqKey: " + task.downloadReqKey + " changed pos from " + pos + " to (" + subORAMIdx + ", " + withinPos + "), match: " + match + ", deliveredAlready: " + deliveredAlready + ", isEviction: " + isEviction, Log.TRACE); }
		}
		localStateLock.unlock(); //---------------------------
		
		Errors.verify(withinPos >= 0, "Coding FAIL: withinPos < 0");
		ReadRemoveWriteInfo rrwi = o.getReadRemoveWriteInfo(withinPos, rng); 
		
		Errors.verify(rrwi != null && rrwi.readSet != null && rrwi.removeSet != null && rrwi.writeSet != null);
		
		Map<Integer, ScheduledOperation> pendingDownloads = new HashMap<>();
		
		
		if(useSemaphoreForPendingOps == true)
		{
			// semaphore -- acquire
			try { pendingDownloadsSemaphore.acquire(rrwi.readSet.size()); } catch(Exception e) { Errors.error(e); }
		}
		
		for(int readPos : rrwi.readSet)
		{
			String objectKey = getObjectKeyFromPos(subORAMIdx, readPos);
			ScheduledOperation sop = s.downloadObject(new DownloadOperation(reqId, objectKey));
			
			pendingDownloads.put(readPos, sop);
		}		
		Pollable.waitForCompletion(pendingDownloads.values()); // wait for downloads to complete
		
		if(useSemaphoreForPendingOps == true)
		{
			pendingDownloadsSemaphore.release(rrwi.readSet.size()); //--- semaphore -- release
		}
		
		// parse and extract the download blocks
		RewriteInfo ri = new RewriteInfo();
		ri.reqKey = effectiveReqKey; // !!
		ri.posToRead = task.posToRead;
		ri.o = o;
		ri.downloads = pendingDownloads;
		ri.removeSet = rrwi.removeSet;
		ri.writeSet = rrwi.writeSet;
		ri.rewriteBlocks = task.toRewrite;
		ri.delivered  = task.listener == null ? null : task.listener.delivered;
		
		prepareRewrite(ri); // let the derived class do the work
		
		Map<Integer, DataItem> rewriteMap = ri.rewriteMap;
		
		// rewrite
		final List<ScheduledOperation> pendingUploads = new ArrayList<>();
		
		if(useSemaphoreForPendingOps == true)
		{
			// semaphore -- acquire
			try { pendingUploadsSemaphore.acquire(rewriteMap.keySet().size()); } catch(Exception e) { Errors.error(e); }
		}
		
		for(int writePos : rewriteMap.keySet())
		{
			String objectKey = getObjectKeyFromPos(subORAMIdx, writePos);
			
			// semaphore
			
			DataItem di = rewriteMap.get(writePos);
			ScheduledOperation sop = s.uploadObject(new UploadOperation(reqId, objectKey, di));
			
			pendingUploads.add(sop);
		}
		
		if(useSemaphoreForPendingOps == true)
		{
			pendingUploadsSemaphore.release(rewriteMap.keySet().size()); //--- semaphore -- release
		}
		
		return new Callable<Void>() 
		{
			@Override
			public Void call() throws Exception 
			{
				Pollable.waitForCompletion(pendingUploads); // wait for uploads to complete
				
				o.releaseOwnership(); // release ownership
				
				return null;
			}
		};
	}
	
	private void process(int subORAMIdx)
	{
		Task task = null;
		try { task = pendingTasks.get(subORAMIdx).take(); } catch (InterruptedException e) { Errors.error(e); }
		Errors.verify(task != null);
		final ScheduledRequest sreq = task.sreq;
		
		Errors.verify(subORAMIdx == task.posToRead.subORAMIdx);
		final String jobKey = "" + subORAMIdx;
		
		Callable<Void> completeCallable = processTask(task.reqId, task); // do retrieval and eviction
		
		BlockDataItem ret = null; boolean waitForDelivery = false;
		if(task.logicalPos != null)
		{
			switch(task.logicalPos)
			{
				case InCache: { ret = task.fromCache;} break;
				case InTransit: { waitForDelivery = true; } break;
				case OnStorage: {  waitForDelivery = true; } break;
				default: Errors.error("Coding FAIL: unknown logical position: " + task.logicalPos); break;
			}
		}
		
		boolean pollAfterCallable = false;
		if(task.completed == false && waitForDelivery == true)
		{
			if(dummyScheme == true)	{ ret = getDummyBlock(); }
			else
			{
				// -d-
				//{ log.append("[FC (process)] req " + task.reqId + " waiting on delivery for key " + task.key + " (" + task.reqKey + ") versionId: " + task.versionId, Log.TRACE);}
				
				try { ret = task.listener.deliveryQueue.poll(1, TimeUnit.MICROSECONDS); } catch (InterruptedException e) { Errors.error(e); } 
				pollAfterCallable = ret == null; // if ret is null, we need to re-poll
				
				// -d-
				//if(ret != null) { log.append("[FC (process)] req " + task.reqId + " done waiting on delivery for key " + task.key + " (" + task.reqKey + ") versionId: " + task.versionId, Log.TRACE);}
			}
		}
		
		if(task.completed == false)	{ log.append("[FC (process)] Regular completion of req " + task.reqId + " with key " + task.key + " (" + task.reqKey + ", versionId: " + task.versionId + ")", Log.TRACE);} 
		
		if(pollAfterCallable == true)
		{
			Errors.verify(ret == null);
			
			if(useExecutorToPoll == true)
			{
				final Task theTask = task;
				executor.submit(new Runnable() 
				{
					@Override
					public void run() 
					{
						BlockDataItem bdi = null;
						try { bdi = theTask.listener.deliveryQueue.take(); } catch (InterruptedException e) { Errors.error(e); } Errors.verify(bdi != null);
						//try { ret = task.listener.deliveryQueue.poll(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Errors.error(e); } Errors.verify(ret != null);
						
						// -d-
						//{ log.append("[FC (process)] req " + theTask.reqId + " done waiting on delivery for key " + theTask.key + " (" + theTask.reqKey + ") versionId: " + theTask.versionId, Log.TRACE);}
						
						completeTask(theTask, bdi);
					}
				});
			}
			else
			{
				try { ret = task.listener.deliveryQueue.take(); } catch (InterruptedException e) { Errors.error(e); } Errors.verify(ret != null);
				//try { ret = task.listener.deliveryQueue.poll(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Errors.error(e); } Errors.verify(ret != null);
				
				// -d-
				//{ log.append("[FC (process)] req " + task.reqId + " done waiting on delivery for key " + task.key + " (" + task.reqKey + ") versionId: " + task.versionId, Log.TRACE);}
				
				completeTask(task, ret);
			}
		}
		else { completeTask(task, ret); }
		
		
		pendingTasksSemaphore.release();
		queue.complete(jobKey);
		
		// make sure we wait for write-back to finish and release ownership of that subORAM
		try { completeCallable.call();  } catch (Exception e) { Errors.error(e); } 
		
		// note: the block has already been added to cache if necessary
		{ log.append("[FC (process)] req " + task.reqId + " is done (jobKey: " + jobKey + ").", Log.TRACE);}
	}
	
	private void completeTask(final Task task, BlockDataItem ret)
	{
		if(task.completed == false)
		{
			final ScheduledRequest sreq = task.sreq;
		
			// return data to user
			if(task.isPut == true) { sreq.onSuccess(new EmptyDataItem()); }
			else { sreq.onSuccess(new SimpleDataItem(ret.getPayload())); }
			
			task.completed = true;
		}
	}
	
	private void schedule(final ScheduledRequest sreq)
	{
		final int reqCount = requestCount.getAndIncrement();
		
		final Request req = sreq.getRequest();
		
		{ log.append("[FC (schedule)] attempting to schedule req " + req.getId() + " (permits: " + pendingTasksSemaphore.availablePermits() + ")", Log.TRACE);}
		// acquire the semaphore
		try { pendingTasksSemaphore.acquire(); } catch (InterruptedException e1) { Errors.error(e1); }
		
		localStateLock.lock(); // ---------------------------
		
		final Task task = prepareTask(sreq, reqCount);

		final int subORAMIdx = task.posToRead.subORAMIdx;
		pendingTasks.get(subORAMIdx).offer(task);
		
		final String jobKey = "" + subORAMIdx;
		
		Callable<Void> c = new Callable<Void>() 
		{
			@Override
			public Void call() { process(subORAMIdx/*sreq, task*/); return null; }
		};
		Job<Void> job = new Job<>("schedule", c);
		queue.scheduleJob(jobKey, job, SchedulingPriority.HIGH);
		
		localStateLock.unlock(); // -------------------------
		
		if(synchronous == true)
		{
			{ log.append("[FC (schedule)] waiting for req " + req.getId() + " to finish.", Log.TRACE);}
			job.waitUntilReady();
		}
	}

	@Override
	public ScheduledRequest scheduleGet(GetRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		schedule(sreq);
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req) 
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		schedule(sreq);
		return sreq;
	}

}
