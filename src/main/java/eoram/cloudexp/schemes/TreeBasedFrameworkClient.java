package eoram.cloudexp.schemes;

import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.Set;
import java.util.TreeMap;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.*;
import eoram.cloudexp.data.encoding.FrameworkHeader;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.primitives.Bucket;
import eoram.cloudexp.schemes.primitives.DummyFactory;
import eoram.cloudexp.schemes.primitives.TreeBasedSubORAM;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.Errors;

/**
 * Implements tree-based (CURIOUS) Framework Client.
 * <p><p>
 * This class uses {@link TreeBasedSubORAM} as subORAM.
 */
public class TreeBasedFrameworkClient extends FrameworkClient<TreeBasedSubORAM> 
{
	protected DummyFactory df = (DummyFactory)this;
	
	protected class TreeFoundPosition implements FoundPosition<Position>
	{
		protected TreeBasedSubORAM o = null;
		protected int subORAMIdx = -1;
		protected int nodeIdx = -1;
		
		protected TreeFoundPosition(TreeBasedSubORAM o, int node)
		{ 
			this.o = o; subORAMIdx = o.getIdx();
			nodeIdx = node;
		}
		
		@Override
		public boolean matches(Position pos) 
		{
			if(pos.getLogical() != LogicalPosition.OnStorage && pos.getLogical() != LogicalPosition.InTransit) { return false; }
			if(pos.subORAMIdx != subORAMIdx) { return false; }
			
			int leaf = pos.withinPos;
			if(o.isLeafPos(leaf) == false) { return false; }
			
			Set<Integer> path = o.pathToLeaf(leaf);
			return path.contains(nodeIdx);
		}
		
		@Override
		public String toString() { return "(FP, " + o.getIdx() + ", " + nodeIdx + ")"; }
	}
	
	protected int k = -1;
	protected int blocksPerBucket = 0;
	protected String strategy = null;
	
	public TreeBasedFrameworkClient(int m, int k, int z, int mtp, String sgy) 
	{
		this(m, k, z, mtp, sgy, false, false, false, false);
	}
	
	public TreeBasedFrameworkClient(int m, int k, int z, int mtp, String sgy, boolean reliab, boolean fc, boolean rndPrefix, boolean uetp) 
	{
		super(m, mtp, false, reliab, fc, rndPrefix, uetp, true); 
		blocksPerBucket = z; 
		Errors.verify(blocksPerBucket >= 1);
		
		dummyScheme = false;
		
		this.k = k;
		Errors.verify(k >= 2 && k <= 64);
		
		strategy = sgy;
	}
	
	@Override
	protected String getNameOfSubORAM() { return TreeBasedSubORAM.name(); }
	

	@Override
	protected int getSubORAMSize() { return subORAMs.get(0).getLeafCount(); }

	@Override
	protected int getSubORAMStateSize() {return subORAMs.get(0).getStateSize();	}
	
	@Override
	protected TreeBasedSubORAM initializeSubORAM(int idx, int capacity, boolean fastInit, Collection<BlockDataItem> blocks)
	{
		if(fastInit == true)
		{
			Errors.verify(blocks != null);
			int requiredCapacity = blocks.size();
			Errors.verify(capacity >= requiredCapacity, "Exceeded SubORAM capacity (need: " + requiredCapacity + " got: " + capacity + ")");
		}
		
		TreeBasedSubORAM ret = new TreeBasedSubORAM(idx, capacity, k, blocksPerBucket, strategy);
		
		if(fastInit == true)
		{
			List<Integer> allPos = ret.getAllPos(); 
			Map<Integer, Bucket> bucketMap = new HashMap<>();
			for(int pos : allPos){ bucketMap.put(pos, new Bucket(df, blocksPerBucket));	}
			
			final boolean randomStateAssignment = true;
			
			if(randomStateAssignment == true)
			{
				
				for(BlockDataItem bdi : blocks)
				{
					FrameworkHeader h = (FrameworkHeader) bdi.getHeader();
					String reqKey = h.getKey();

					int leaf = ret.getRandomLeaf(rng);
					TreeSet<Integer> path = new TreeSet<>(ret.pathToLeaf(leaf));

					boolean placed = false;
					for(int node : path.descendingSet())
					{
						Errors.verify(allPos.contains(node) == true);
						Bucket bucket = bucketMap.get(node);
						Errors.verify(bucket != null);

						if(bucket.isFull() == false)
						{
							bucket.add(bdi);
							placed = true;
							break;
						}
					}

					Errors.verify(placed == true, "Couldn't place block!");

					posMap.setPos(reqKey, new Position(idx, leaf));
		        }
			}
			else
			{
				List<Integer> spots = new ArrayList<>();
				for(int pos : allPos) { if(ret.isLeafPos(pos)) { for(int i=0; i<blocksPerBucket; i++) { spots.add(pos); } } }
				Errors.verify(spots.size() >= blocks.size(), "Overflow, we can't place all blocks in leaves!");
				Collections.shuffle(spots, rng);
				
				for(BlockDataItem bdi : blocks)
				{
					FrameworkHeader h = (FrameworkHeader) bdi.getHeader();
					String reqKey = h.getKey();
					
					int node = spots.remove(0);
					Bucket bucket = bucketMap.get(node);
					Errors.verify(bucket != null && bucket.isFull() == false);
					
					bucket.add(bdi);
					
					posMap.setPos(reqKey, new Position(idx, node));
				}
			}
			
			List<ScheduledOperation> p = new ArrayList<>();
			for(int pos : allPos)
			{
				Bucket bucket = bucketMap.get(pos);
				String objectKey = getObjectKeyFromPos(idx, pos);
				ScheduledOperation sop = s.uploadObject(new UploadOperation(Request.initReqId, objectKey, bucket.getEncryption()));
				p.add(sop);
			}
			Pollable.waitForCompletion(p);
		}
		
		return ret;
	}
	
	@Override
	protected TreeBasedSubORAM loadSubORAM(ObjectInputStream is) throws Exception 
	{
		TreeBasedSubORAM ret = new TreeBasedSubORAM(is);
		return ret;
	}	
	
	@Override
	protected int getMaxEvictCount(int occupancy, int capacity) 
	{
		return blocksPerBucket; 
		// int max = super.getMaxEvictCount(occupancy, capacity);
		//	if(max > blocksPerBucket) { max = blocksPerBucket; }
		//	return max;
	}
	
	private int getDeepestPos(TreeBasedSubORAM o, int leaf, Set<Integer> rewritePath)
	{
		Set<Integer> path = o.pathToLeaf(leaf);
		TreeSet<Integer> intersection = new TreeSet<>(path);
		intersection.retainAll(rewritePath); 
		Errors.verify(intersection.isEmpty() == false);
		
		int deepestPossiblePos = intersection.last();
		return deepestPossiblePos;
	}
	
	protected class FoundBlockInfo
	{
		protected int foundPos = -1;
		protected BlockDataItem bdi = null;
	}
	
	@Override
	protected void prepareRewrite(RewriteInfo ri)
	{
		int addedCount = 0;
		final int subORAMIdx = ri.o.getIdx();
		
		Errors.verify(ri.removeSet.isEmpty() == true); // this should be empty
		
		ri.rewriteMap = new TreeMap<Integer, DataItem>();
		Map<Integer, Bucket> map = new TreeMap<>();
		
		// -d-
		//if(ss.debug == true){ log.append("[OFC] SubORAM " + subORAMIdx + " downloaded path: " + ri.downloads.keySet() + ", rewrite path: " + ri.writeSet, Log.TRACE); }
		
		TreeMap<Integer, Entry<BlockDataItem, Integer>> blocksFoundWithKey = new TreeMap<>();
		for(int withinPos : ri.downloads.keySet())
		{
			ScheduledOperation sop = ri.downloads.get(withinPos);
			Errors.verify(sop.isReady() == true && sop.wasSuccessful() == true);
			
			Bucket bucket = new Bucket(df, blocksPerBucket, sop.getDataItem());
			Iterator<BlockDataItem> iter = bucket.getBlocks().iterator();
			while(iter.hasNext() == true)
			{
				BlockDataItem bdi = iter.next();
				FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
				String foundReqKey = header.getKey();
				if(isDummy(foundReqKey) == false && foundReqKey.equals(ri.reqKey))
				{
					blocksFoundWithKey.put(header.getVersionId(), new AbstractMap.SimpleEntry<>(bdi, withinPos)); // add to found set, then remove					
					iter.remove();
				}
				
				// -d-
				//{ log.append("[OFC] Found block with key " + header.getKey() + ", versionId: " + header.getVersionId() + " at pos: " + withinPos, Log.TRACE); }
			}
			
			if(ri.writeSet.contains(withinPos) == true) { map.put(withinPos, bucket); }
		}
		
		boolean found = false;
		
		long start = System.currentTimeMillis();
		
		localStateLock.lock(); // -----------------------
		
		// -d-
		//{ log.append("[OFC] step 1 (blocksFoundWithKey) for " + ri.reqKey, Log.TRACE); }
				
		// go over entries in descending order of version
		for(int versionId : blocksFoundWithKey.descendingKeySet())
		{
			Entry<BlockDataItem, Integer> entry = blocksFoundWithKey.get(versionId);
			BlockDataItem bdi = entry.getKey();
			int pos = entry.getValue();
			
			Position newPos = (found == false) ? new Position(LogicalPosition.InCache) : null;
			
			TreeFoundPosition foundPos = new TreeFoundPosition(ri.o, pos);
			
			// instead of onMove, go with onFind, block will be moved to cache if necessary
			//onMove(bdi, foundPos, newPos); 
			onFind(bdi, foundPos, true, !found);
			
			found = true;
		}
		
		
		if(isDummy(ri.reqKey) == false && found == false)
		{
			if(ri.delivered == null || ri.delivered.get() == false) 
			{ 
				Errors.error("[OFC] couldn't find key " + ri.reqKey + " on the path " + ri.downloads.keySet());
			}
		}
		
		// -d-
		//{ log.append("[OFC] step 2 (foundBlocks) for " + ri.reqKey, Log.TRACE); }
		
		Map<String, TreeMap<Integer, FoundBlockInfo>> foundBlocks = new HashMap<>();
		for(int rewritePos : map.keySet())
		{
			Bucket bucket = map.get(rewritePos);
			Errors.verify(bucket != null);
			// -d-
			//{ log.append("[OFC] Found bucket " + bucket + " at rewritePos: " + rewritePos, Log.TRACE); }
			
			for(BlockDataItem bdi : bucket.getBlocks())
			{
				FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
				String thisReqKey = header.getKey();
				if(isDummy(thisReqKey) == true) { continue; } // ignore dummy blocks
				int versionId = header.getVersionId();
				
				boolean addToFound = true;
				/*for(BlockDataItem evictBDI : ri.rewriteBlocks)
				{
					// don't add to found old versions of blocks we are about to evict
					FrameworkHeader eh = ((FrameworkHeader)evictBDI.getHeader());
					String evictReqKey = eh.getKey();
					if(thisReqKey.equals(evictReqKey) == true) 
					{ if(versionId < eh.getVersionId()) { addToFound = false; break; } } 
				}*/
				
				if(addToFound == true) 
				{
					TreeMap<Integer, FoundBlockInfo> tm = foundBlocks.get(thisReqKey);
					if(tm == null)
					{
						foundBlocks.put(thisReqKey, new TreeMap<Integer, FoundBlockInfo>());
						tm = foundBlocks.get(thisReqKey);
					}
					FoundBlockInfo fbi = new FoundBlockInfo();
					fbi.bdi = bdi; fbi.foundPos = rewritePos;
					
					boolean add = true;
					if(tm.containsKey(versionId) == true)
					{
						FoundBlockInfo otherFBI = tm.get(versionId);
						add = otherFBI.foundPos > rewritePos;
						
						// -d-
						//{ log.append("[OFC] Found conflicting block with key " + thisReqKey + ", versionId: " + versionId + " at rewritePos: " + rewritePos + " (subORAM " + subORAMIdx + "), other is at: " + otherFBI.foundPos, Log.TRACE); }
					}
					
					if(add == true) { tm.put(versionId, fbi); }
					
					// -d-
					//{ log.append("[OFC] Found block with key " + thisReqKey + ", versionId: " + versionId + " at rewritePos: " + rewritePos + " (subORAM " + subORAMIdx + "), add: " + add, Log.TRACE); }
				}
				else
				{
					;
					// -d-
					//{ log.append("[OFC] Discarded block with key " + thisReqKey + ", versionId: " + versionId, Log.TRACE); }
				}
			}
			bucket.clear();
		}
		
		
		Set<Entry<Integer, BlockDataItem>> rewriteBlocksSet = new HashSet<>();
		
		Integer[] rewritePosArray = ri.writeSet.toArray(new Integer[ri.writeSet.size()]);
		for(int i = rewritePosArray.length-1; i>= 0; i--)
		{
			int rewritePos = rewritePosArray[i]; Bucket bucket = map.get(rewritePos);
			if(bucket == null) { map.put(rewritePos, new Bucket(df, blocksPerBucket)); }
		}
		

		// -d-
		//{ log.append("[OFC] step 3 (foundBlocks - discard) for " + ri.reqKey, Log.TRACE); }
		
		for(String thisReqKey : foundBlocks.keySet()) //TODO: optimize locking
		{
			TreeMap<Integer, FoundBlockInfo> tm = foundBlocks.get(thisReqKey);
			boolean mostRecent = true;
			for(int versionId : tm.descendingKeySet())
			{
				FoundBlockInfo fbi = tm.get(versionId);
				BlockDataItem bdi = fbi.bdi; FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
				int foundPos = fbi.foundPos;
				
				// -d-
				//{ log.append("[OFC] Processing block with key " + thisReqKey + " (versionId: " + versionId + ", hvid: " + header.getVersionId() + "), foundPos: " + foundPos, Log.TRACE); }
				
				TreeFoundPosition tfPos = new TreeFoundPosition(ri.o, foundPos);
				Entry<Boolean, Position> e = onFind(bdi, tfPos, true, !mostRecent);//false);
				boolean keep = e.getKey();
				Position currentPos = e.getValue();
				boolean pushDown = tfPos.matches(currentPos);
				
				if(keep == true && mostRecent == true)
				{
					int deepestPossiblePos = foundPos;
					if(pushDown == true) // push this block as deep as possible
					{ deepestPossiblePos = getDeepestPos(ri.o, currentPos.withinPos, ri.writeSet); }
					rewriteBlocksSet.add(new AbstractMap.SimpleEntry<>(deepestPossiblePos, bdi));
				}
				else
				{
					;
					// -d-
					//{ log.append("[OFC] Discarded block with key " + thisReqKey + " (versionId: " + versionId + "), tfPos: " + tfPos + ", currentPos: " + currentPos + " (keep: " + keep + ", mostRecent: " + mostRecent + ")", Log.TRACE); }
				}
				
				mostRecent = false;
			} 
		}
		
		// -d-
		//{ log.append("[OFC] step 4 (mustRewriteBlocksSet) for " + ri.reqKey, Log.TRACE); }
		
		Set<Entry<Integer, BlockDataItem>> mustRewriteBlocksSet = new HashSet<>();
		for(BlockDataItem bdi : ri.rewriteBlocks)
		{
			FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
			String thisReqKey = header.getKey();
			int versionId = header.getVersionId();
			Errors.verify(isDummy(thisReqKey) == false);
			
			int leaf = ri.o.getRandomLeaf(rng);
			
			Position evictToPos = new Position(subORAMIdx, -versionId);
			DefaultFoundPosition foundPos = new DefaultFoundPosition(evictToPos);
			
			Position newPos = new Position(subORAMIdx, leaf);
			boolean moved = onMove(bdi, foundPos, newPos);
			
			if(moved == true)
			{
				int deepestPossiblePos = getDeepestPos(ri.o, leaf, ri.writeSet);
				mustRewriteBlocksSet.add(new AbstractMap.SimpleEntry<>(deepestPossiblePos, bdi));
			}
			else 
			{ 
				;
				// -d-
				//{ log.append("[OFC] Discarded evict block with key " + thisReqKey + ", versionId: " + versionId, Log.TRACE); } 
			}
		}
		
		// first the blocks that MUST be rewritten
		for(int i = rewritePosArray.length-1; i>= 0; i--)
		{
			int rewritePos = rewritePosArray[i];
			Bucket bucket = map.get(rewritePos); Errors.verify(bucket != null);
			
			Iterator<Entry<Integer, BlockDataItem>> iter = mustRewriteBlocksSet.iterator();
			while(iter.hasNext() == true)
			{
				Entry<Integer, BlockDataItem> entry = iter.next();
				int deepestPossiblePos = entry.getKey(); BlockDataItem bdi = entry.getValue();
				
				if(deepestPossiblePos >= rewritePos)
				{
					if(bucket.isFull() == false)
					{ bucket.add(bdi); iter.remove(); }
				}
			}
		}
		Errors.verify(mustRewriteBlocksSet.isEmpty() == true, "Coding FAIL: mustRewriteBlocksSet is not empty!");
		
		// now the other blocks to rewrite
		for(int i = rewritePosArray.length-1; i>= 0; i--)
		{
			int rewritePos = rewritePosArray[i];
			Bucket bucket = map.get(rewritePos); Errors.verify(bucket != null);
			
			Iterator<Entry<Integer, BlockDataItem>> iter = rewriteBlocksSet.iterator();
			while(iter.hasNext() == true)
			{
				Entry<Integer, BlockDataItem> entry = iter.next();
				int deepestPossiblePos = entry.getKey(); BlockDataItem bdi = entry.getValue();
				
				if(deepestPossiblePos >= rewritePos)
				{
					if(bucket.isFull() == false)
					{ bucket.add(bdi); iter.remove(); }
				}
			}
		}
		
		// -d-
		//{ log.append("[OFC] step 5 (rewriteBlocksSet - overflow) for " + ri.reqKey, Log.TRACE); }
		
		// for the remaining blocks we couldn't place, put them back in eviction cache
		for(Entry<Integer, BlockDataItem> entry : rewriteBlocksSet)
		{
			BlockDataItem bdi = entry.getValue();
			int rewritePos = entry.getKey();
			FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
			String thisReqKey = header.getKey();
			Errors.verify(isDummy(thisReqKey) == false);
			
			TreeFoundPosition tfPos = new TreeFoundPosition(ri.o, rewritePos);
			Position newPos = new Position(LogicalPosition.InCache);
			boolean moved = onMove(bdi, tfPos, newPos);
			addedCount += moved == true ? 1 : 0;
		}
		localStateLock.unlock(); // -----------------------		
		
		long end = System.currentTimeMillis();
		{ log.append("[OFC] Time to process rewrite on subORAM " + subORAMIdx + ": " + (end - start) + " ms.", Log.TRACE); }
		
		ri.rewriteMap = new TreeMap<>();
		for(int rewritePos : map.keySet()) 
		{ 
			Bucket bucket = map.get(rewritePos);
			ri.rewriteMap.put(rewritePos, bucket.getEncryption());
		}
		
		// -d-
		//{ log.append("[OFC] Query for key " + ri.reqKey + ", addedCount: " + addedCount, Log.TRACE); }
	}

}
