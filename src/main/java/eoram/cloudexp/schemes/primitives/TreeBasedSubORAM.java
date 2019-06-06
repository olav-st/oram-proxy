package eoram.cloudexp.schemes.primitives;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import eoram.cloudexp.utils.Errors;

/**
 * Implementation of the tree-based subORAM module for the (CURIOUS) Framework Client.
 *
 */
public class TreeBasedSubORAM implements FrameworkSubORAM
{	
	protected Semaphore semaphore = null;
	
	protected int idx = 0;
	protected int size = 0;
	protected int capacity = 0;
	protected int k = 0;
	protected int z = 0;
	protected int depth = 0;
	protected int nodes = 0;
	protected int firstLeafIdx = 0;
	protected int lastLeafIdx = 0;
	protected int lastLevelNodes = 0;

	protected String strategy = null;
	
	protected int readCount = 0;
	protected BitSet existingNodes = null;
	
	public TreeBasedSubORAM(int id, int c, int k, int z, String sgy)
	{
		strategy = sgy;
		
		idx = id; capacity = c; this.k = k; this.z = z;
		size = 0; 
		depth = 0;
		
		Errors.verify(capacity > 0);
		
		int lowerLevelsCap = 0;
		while(lowerLevelsCap < capacity)
		{
			int nodesInLevel = (int)Math.pow(k, depth);
			size += z * nodesInLevel;
			
			lowerLevelsCap = nodesInLevel * z;
			if(depth > 0) { lowerLevelsCap += (int)Math.pow(k, depth-1) * z; }
			
			boolean capacityMet = lowerLevelsCap >= capacity;
			
			if(capacityMet == true) { firstLeafIdx = nodes; }
			nodes += nodesInLevel;
			
			if(capacityMet == true) { lastLeafIdx = nodes-1; }
			
			if(capacityMet == false) { depth++; }
		}
		
		lastLevelNodes = lastLeafIdx - firstLeafIdx + 1;
		readCount = 0;
		
		existingNodes = new BitSet(lastLeafIdx+1);
		
		semaphore = new Semaphore(1);
	}
	
	@Override
	public synchronized int getIdx() { return idx; }

	public synchronized int getSize() { return size; }

	public synchronized int getCapacity() { return capacity; }
	
	
	@Override
	public int getDummyPos() { return 0; }
	
	@Override
	public boolean isDummyPos(int pos) { return (pos == 0); }
	
	
	public Set<Integer> pathToLeaf(int leafIdx) 
	{
		Set<Integer> nodesList = new TreeSet<Integer>();
		int nodeIdx = leafIdx;
		while(nodeIdx >= 0)
		{
			nodesList.add(nodeIdx);
			nodeIdx = (int)Math.floor((nodeIdx - 1.0) / (double)k);
		}
		return nodesList;
	}

	public synchronized int getRandomLeaf(Random rng)
	{
		return firstLeafIdx + rng.nextInt(lastLevelNodes);
	}
	
	public synchronized boolean isLeafPos(int pos) { return (pos >= firstLeafIdx && pos <= lastLeafIdx); }


	public int getLeafCount() { return lastLevelNodes; }
	public int getStateSize() { return depth+1 + existingNodes.size(); }
	
	private Set<Integer> filterByExisting(Set<Integer> path)
	{
		Set<Integer> ret = new TreeSet<Integer>();
		for(int pos : path)	{ if(existingNodes.get(pos) == true) { ret.add(pos); } }
		return ret;
	}
	
	private void ensureExisting(Set<Integer> path)
	{
		for(int pos : path) { existingNodes.set(pos); }
	}
	
	private Entry<Set<Integer>, Set<Integer>> getPosForReadAndReWrite(int pos, Random rng)
	{
		if(isDummyPos(pos)) { pos = getRandomLeaf(rng); }
		Errors.verify(isLeafPos(pos) == true);
		
		Set<Integer> readPath = getPosForRead(pos, rng); 
		Set<Integer> reWriteSet = getPosForReWrite(readPath);
		
		Set<Integer> filteredReadPath = filterByExisting(readPath);
		ensureExisting(reWriteSet);
		
		return new AbstractMap.SimpleEntry<>(filteredReadPath, reWriteSet);		
	}
	
	private Set<Integer> getPosForRead(int pos, Random r)
	{
		Set<Integer> ret = pathToLeaf(pos);
		readCount++;
		
		return ret;
	}
	
	private Set<Integer> getPosForReWrite(Set<Integer> readPath)
	{		 
		Set<Integer> ret = new TreeSet<>(); int i = 0;
		for(int nodeIdx : readPath)
		{
			int m = 1; // default strategy
			
			if(strategy.equals("default")) { m = 1; }
			else if(strategy.equals("half")) 
			{
				m = (i < 2) ? 1 : 2;
			}
			else if(strategy.equals("log"))
			{
				m = 1+(int)Math.floor(Math.log(i < 2 ? 1.0 : i-1)/Math.log(k));
			}
			else if(strategy.equals("stairs"))
			{
				m = 1+i/2;
				if(m > 3) { m = 3; }
			}
			
			if((readCount % m) == 0) { ret.add(nodeIdx); }
			
			i++;
		}
		return ret;
	}
	
	public synchronized String toString()
	{
		String ret = "";
		ret += "k: " + k + ", z:" + z + ", nodes: " + nodes + ", capacity: " + capacity;
		
		return ret;
	}

	@Override
	public synchronized ReadRemoveWriteInfo getReadRemoveWriteInfo(int pos, Random rng) 
	{
		ReadRemoveWriteInfo ret = new ReadRemoveWriteInfo();
		Entry<Set<Integer>, Set<Integer>> r = getPosForReadAndReWrite(pos, rng);
		
		ret.readSet = r.getKey();
		ret.removeSet = new TreeSet<>();
		ret.writeSet = r.getValue();
		
		return ret;
	}

	public static String name() { return "OptimizedSubORAM"; }
	
	@Override
	public String getName() { return "OptimizedSubORAM"; }

	@Override
	public void acquireOwnership() 
	{
		// simple coarse-grained permit using a semaphore
		try { semaphore.acquire(); } catch(Exception e) { Errors.error(e); }
	}


	@Override
	public void releaseOwnership()
	{
		try { semaphore.release(); } catch(Exception e) { Errors.error(e); }
	}


	public List<Integer> getAllPos() 
	{
		List<Integer> ret = new ArrayList<Integer>();
		
		for(int nodeIdx = 0; nodeIdx <= lastLeafIdx; nodeIdx++)
		{
			ret.add(nodeIdx);
			existingNodes.set(nodeIdx);
		}
		
		return ret;
	}

	public TreeBasedSubORAM(ObjectInputStream is) throws Exception
	{
		idx = is.readInt();
		size = is.readInt();
		capacity = is.readInt();
		k = is.readInt();
		z = is.readInt();
		depth = is.readInt();
		nodes = is.readInt();
		firstLeafIdx = is.readInt();
		lastLeafIdx = is.readInt();
		lastLevelNodes = is.readInt();
		
		strategy = (String)is.readObject();
		
		readCount = is.readInt();
		existingNodes = (BitSet)is.readObject();
		
		semaphore = new Semaphore(1);
	}
	
	@Override
	public void save(ObjectOutputStream os) throws Exception 
	{
		os.writeInt(idx);
		os.writeInt(size);
		os.writeInt(capacity);
		os.writeInt(k);
		os.writeInt(z);
		os.writeInt(depth);
		os.writeInt(nodes);
		os.writeInt(firstLeafIdx);
		os.writeInt(lastLeafIdx);
		os.writeInt(lastLevelNodes);
		
		os.writeObject(strategy);
		
		os.writeInt(readCount);
		os.writeObject(existingNodes);
	}
	
	
	@Override
	public int[] getReadWriteCounts() 
	{
		int[] ret = new int[4];
		
		int meanWrites = depth+1;
		
		if(strategy.equals("half") || strategy.equals("stairs") || strategy.equals("log")) 
		{ meanWrites = (int)Math.ceil(2.0*meanWrites/3.0); }
		
		ret[0] = depth+1;
		ret[1] = depth+1;
		ret[2] = meanWrites;
		ret[3] = depth+1;
		
		return ret;
	}
}
