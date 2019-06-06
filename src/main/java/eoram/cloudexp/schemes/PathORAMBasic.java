package eoram.cloudexp.schemes;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.schemes.primitives.Tree;
import eoram.cloudexp.schemes.primitives.TreeBasedUtils;
import eoram.cloudexp.service.DownloadOperation;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.Errors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.SecureRandom;
import java.util.*;

/**
 * Implements basic PathORAM logic (see Stefanov, Emil, et al. "Path oram: An extremely simple oblivious ram protocol." ACM CCS 2013.).
 * <p><p>
 * This implementation is based on Java code obtained from authors of a follow-up work.
 * <p>
 *
 */
public class PathORAMBasic
{
	protected Log log = Log.getInstance();
	
	protected SecureRandom rnd;
	int dataSize; // measured in byte.
	int extDataSize; // measured in byte.
	static final int keyLen = 10; // length of key (to encrypt each data piece) in bytes
	static final int nonceLen = 10; // length of nonce (to encrypt each data piece) in bytes

	public static int Z = 4;
	public static int stashSize = 89; // set according to table 3 of PathORAM paper (for 80-bit security)
	
	public static int C = 1024;
	//public static int C = 4; // memory reduction factor (ensure it is a power of 2)
	
	public static final boolean stashUseLS = false;
	//public static final boolean stashUseLS = true;
	
	Tree serverTree;
	int recLevel;
	
	byte[] clientKey;
	
	public PathORAMBasic (SecureRandom rand) 
	{
		rnd = rand;
		clientKey = TreeBasedUtils.genPRBits(rnd, keyLen);
	}
	
	/*
	 * Each data has 'unitB' bytes.
	 * 'data' will be intact since the real block has a clone of each element in 'data'.
	 */
	public BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int unitB, BitSet[] data, int recLevel) {
		assert(maxBlocks < (~(1<<63))) : "Too many blocks in a tree.";
		
		this.recLevel = recLevel;
		int nextB = unitB;
		serverTree = new Tree(rnd, C, Z, recLevel, stashSize, stashUseLS, nonceLen, clientKey);
		BitSet[] posMap = serverTree.initialize(si, maxBlocks, nextB, data);

		return posMap;
	}

	public enum OpType {Read, Write};
	protected Tree.Block access(long reqId, BitSet[] posMap, OpType op, int a, BitSet data)
	{
		Tree tr = serverTree;

		int leafLabel = tr.N-1 + TreeBasedUtils.readPositionMap(C, posMap, tr, a);
		Tree.Block[] blocks = tr.readBuckets(reqId, leafLabel);
		
		int newLabel = rnd.nextInt(tr.N);
		TreeBasedUtils.writePositionMap(C, posMap, tr, a, newLabel); //(u % s.C)*s.D, newlabel, s.D);
		
		// debug only
		//{ log.append("[POB (access)] (R" + recLevel + ") Block with id " + a + " should be in the path down to leaf " + leafLabel + " (label: " + (leafLabel - (tr.N-1)) + ") or in the stash (new label: " + newLabel + ").", Log.TRACE); }
		
		return rearrangeBlocksAndReturn(reqId, op, a, data, leafLabel, blocks, newLabel);
	}

	protected Tree.Block rearrangeBlocksAndReturn(long reqID, OpType op, int a, BitSet data, int leafLabel, Tree.Block[] blocks, int newlabel)
	{
		Tree tr = serverTree;
		Tree.Block res = null;
		Tree.Block[] union = new Tree.Block[tr.stash.size+blocks.length];
		
		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();
		
		if(tr.stash.ls != null) // use LS
		{
			for (int i = 0; i < tr.stash.size; i++) 
			{
				String objectKey = recLevel+"#"+(i);
				DownloadOperation download = new DownloadOperation(reqID, objectKey);
				ScheduledOperation sop = tr.stash.ls.downloadObject(download);
				v.add(sop);
			}
			Pollable.waitForCompletion(v);
			for (int i = 0; i < tr.stash.size; i++) { union[i] = tr.new Block(v.get(i).getDataItem().getData(), true);}
		}
		else
		{
			for (int i = 0; i < tr.stash.size; i++)
			{
				Tree.Block blk = null;
				if(i < tr.stash.blocks.size()) { blk = tr.stash.blocks.get(i); }
				else { blk = tr.new Block(); }
				union[i] = blk;
			}
		}
		
		for (int i = 0; i < blocks.length; i++) { union[i+tr.stash.size] = tr.new Block(blocks[i]); }
		
		for (Tree.Block blk : union)
		{
			if (blk.r != null) // when r == null, the block comes from the stash, already decrypted.
			{ blk.dec(); }
		}
		for (int i = 0; i < union.length; i++)
		{
			Tree.Block blk = union[i];
			if (blk.id == a) {
				res = tr.new Block(blk);
				blk.treeLabel = newlabel;
				if (op == OpType.Write) {
					blk.data = data;
				}
			}
		}
		
		/*
		{ // debug only
			for (int i = 0; i < tr.stash.size; i++) 
			{ 
				Tree.Block blk = union[i]; 
				if(blk.isDummy() == false) 
				{ log.append("[POB (rearrangeBlocksAndReturn)] Stash contains block with id " + blk.id, Log.TRACE); }
			}
			for (int i = 0; i < blocks.length; i++) 
			{ 
				Tree.Block blk = union[i+tr.stash.size]; 
				if(blk.isDummy() == false) { log.append("[POB (rearrangeBlocksAndReturn)] Path down to label " + (leafLabel - (tr.N-1)) + " contain block with id " + blk.id, Log.TRACE); }
			}
		}*/
		if(res == null)
		{
			Errors.error("[POB (rearrangeBlocksAndReturn)] Couldn't find block with id " + a + " in the path down to label " + (leafLabel - (tr.N-1))+ " or in the stash.");
		}
		
		List<Tree.Block> unionList = Arrays.asList(union);
		Collections.shuffle(unionList, rnd);
		union = unionList.toArray(new Tree.Block[0]);
		
		/** // this code is equivalent to (and easier to follow than) the piece right below
		 
		v.clear();
		Set<Integer> pathSet = getPathString(leafLabel); System.out.println(pathSet);
		Map<Integer, Tree.Bucket> pathMap = new TreeMap<Integer, Tree.Bucket>();
		
		for(int node : pathSet) { pathMap.put(node, tr.new Bucket(tr.new Block())); }
		
		int failedToEvictCount = 0;
		for (int j = 0; j < union.length; j++) 
		{
			Tree.Block thisBlock = union[j];
			if (thisBlock.isDummy() == false)
			{ 
				Set<Integer> assignedPathSet = getPathString(thisBlock.treeLabel+tr.N-1);
				
				Set<Integer> intersection = new HashSet<Integer>(pathSet);
				intersection.retainAll(assignedPathSet);
				
				int max = Collections.max(intersection);
				
				//System.out.println("\t" + intersection + " -> " + max + "\t(" + assignedPathSet + ")");
				
				Tree.Bucket bucket = pathMap.get(max);
				assert(bucket != null);

				boolean failedToEvict = true;
				for(int k=0; k < Z; k++) 
				{
					if(bucket.blocks[k].isDummy() == true)
					{
						bucket.blocks[k] = tr.new Block(thisBlock);
						thisBlock.erase(); 
						failedToEvict = false;
						break;
					}
				}
				
				if(failedToEvict == true) { failedToEvictCount++; }
			}
		}
		
		//System.out.println("failedToEvictCount: " + failedToEvictCount);
		
		for(int node : pathMap.keySet())
		{		
			Tree.Bucket bucket = pathMap.get(node); assert(bucket != null);
			bucket.encryptBlocks();
		
			String objectKey = recLevel + "#" + node;
			DataItem di = new SimpleDataItem(bucket.toByteArray());
			UploadOperation upload = new UploadOperation(reqID, objectKey, di);
			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
			//System.out.print("objectKey: " + objectKey + " ");
			
			v.add(sop);
		}
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		**/
		
		
		v.clear();
		for (int i = tr.D+1; i > 0; i--) 
		{
			int prefix = TreeBasedUtils.iBitsPrefix(leafLabel+1, tr.D+1, i);
			
			String objectKey = recLevel + "#" + ((prefix>>(tr.D+1-i))-1);
			
			Tree.Bucket bucket = tr.new Bucket(tr.new Block());
			for (int j = 0, k = 0; j < union.length && k < Z; j++) 
			{
				if (!union[j].isDummy())
				{ 
					int jprefix = TreeBasedUtils.iBitsPrefix(union[j].treeLabel+tr.N, tr.D+1, i);
					
					if(prefix == jprefix) 
					{
						bucket.blocks[k++] = tr.new Block(union[j]);
						
						// debug only
						//{ log.append("[POB (rearrangeBlocksAndReturn)] Block with id " + union[j].id + " will be re-written in bucket on the path to label " + union[j].treeLabel + " (objectKey: " + objectKey + ")", Log.TRACE); }
						
						union[j].erase();
					}
				}
			}
			
			
			/*for(int k=0; k<Z; k++) // debug only
			{
				Tree.Block blk = bucket.blocks[k];
				if(blk.isDummy() == false)
				{
					{ log.append("[POB (rearrangeBlocksAndReturn)] Bucket " + objectKey + " contains block with id " + blk.id + " with label " + blk.treeLabel, Log.TRACE); }
				}
			}*/
			
			bucket.encryptBlocks();
			
			DataItem di = new SimpleDataItem(bucket.toByteArray());
			UploadOperation upload = new UploadOperation(reqID, objectKey, di);
			ScheduledOperation sop = tr.storedTree.uploadObject(upload);
			
			v.add(sop);
		}
		Pollable.waitForCompletion(v); v.clear(); // wait and clear
		
		
		if(tr.stash.ls == null) { tr.stash.blocks.clear(); } // clear stash in memory
		
		
		// put the rest of the blocks in 'union' into the 'stash'
		int j = 0, k = 0;
		for (; j < union.length && k < tr.stash.size; j++) 
		{
			if (union[j].isDummy() == false) 
			{
				if(tr.stash.ls != null) // use LS
				{
					String objectKey = recLevel + "#" + (k);
					DataItem di = new SimpleDataItem(union[j].toByteArray());
					UploadOperation upload = new UploadOperation(reqID, objectKey, di);
					ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
					sop.waitUntilReady();
				}
				else
				{
					tr.stash.blocks.add(tr.new Block(union[j]));
				}
				
				k++;

				union[j].erase();
			}
		}
		if (k == tr.stash.size) 
		{
			for (; j < union.length; j++)
			{ assert (union[j].isDummy()) : "Stash is overflown: " + tr.stash.size; }	
		}
		
		if(tr.stash.ls != null) // use LS
		{
			while (k < tr.stash.size) 
			{
				String objectKey = recLevel + "#" + (k);
				DataItem di = new SimpleDataItem(tr.new Block().toByteArray());
				UploadOperation upload = new UploadOperation(reqID, objectKey, di);
				ScheduledOperation sop = tr.stash.ls.uploadObject(upload);
				sop.waitUntilReady();
		
				k++;
			}
		}
		
		return res;
	}
	
	private Set<Integer> getPathString(int leaf) 
	{
		Set<Integer> nodesList = new TreeSet<Integer>();
		int temp = leaf;
		while(temp >= 0)
		{
			nodesList.add(temp);
			temp = ((temp+1)>>1)-1;
		}
		return nodesList;
	}

	Tree.Block read(long reqID, BitSet[] pm, int i) { return access(reqID, pm, OpType.Read, i, null); }
	
	Tree.Block write(long reqID, BitSet[] pm, int i, BitSet d) { return access(reqID, pm, OpType.Write, i, d); }

	protected void recursiveSave(ObjectOutputStream os) throws IOException
	{
		os.writeInt(dataSize);
		os.writeInt(extDataSize);
		//os.writeInt(keyLen); os.writeInt(nonceLen);
		os.writeInt(Z); os.writeInt(C);
		os.writeInt(stashSize);
		
		serverTree.save(os);
		
		os.writeInt(recLevel);
		
		os.write(clientKey);		
	}
	
	protected void initializeLoad(ExternalStorageInterface si, ObjectInputStream is) throws IOException
	{
		dataSize = is.readInt();
		extDataSize = is.readInt();
		
		//keyLen = is.readInt(); nonceLen = is.readInt();
		Z = is.readInt(); C = is.readInt();
		stashSize = is.readInt();
		
		serverTree = new Tree(si, is);
		
		recLevel = is.readInt();
		clientKey = new byte[keyLen];
		is.readFully(clientKey);
	}

	protected void recursiveLoad(ExternalStorageInterface esi, ObjectInputStream is, int i) throws IOException 
	{
		initializeLoad(esi, is);		
	}

	public int getRecursionLevels() { return 0; }
}