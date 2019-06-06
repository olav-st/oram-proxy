package eoram.cloudexp.schemes.primitives;

import java.io.ObjectInputStream;

import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.BlockDataItem;
import eoram.cloudexp.data.encoding.FrameworkHeader;
import eoram.cloudexp.utils.Errors;
//import eoram.cloudexp.utils.MiscUtils;

/**
 * Implements a slotted eviction cache
 */
public class EvictionCache 
{		
	protected Log log = Log.getInstance();
	
	protected Random rng = null;
	
	protected int capacity = 0;
	protected int peakOccupancy = 0;
	
	protected Map<String, BlockDataItem> cache = new HashMap<String, BlockDataItem>();
	
	protected List<List<String>> slots = null;
	protected Map<String, Integer> slotsMap = null;
	
	public EvictionCache(Random r, int c) 
	{ 
		rng = r;
		capacity = c; peakOccupancy = 0;

		slots = new ArrayList<>();
		for(int i=0; i<c; i++) { slots.add(new ArrayList<String>()); }
		
		slotsMap = new HashMap<String, Integer>();
	}
	
	public synchronized int getOccupancy() { return cache.size(); }
	public synchronized int getCapacity() { return capacity; }
	
	public synchronized void clear() { cache.clear(); }
	
	public synchronized boolean put(String key, BlockDataItem bdi) 
	{
		boolean ret = false;
		
		Errors.verify(key != null && bdi != null);
		FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
		Errors.verify(key.equals(header.getKey()));
		int newVersionId = header.getVersionId(); int existingVersionId = -1;
		
		if(cache.containsKey(key) == true)
		{
			FrameworkHeader existingHeader = (FrameworkHeader)cache.get(key).getHeader();
			existingVersionId = existingHeader.getVersionId();
		}
		
		if(existingVersionId <= newVersionId)
		{
			cache.put(key, bdi); 
			ret = true;
			
			final int sz = cache.size();
			
			if(slotsMap.containsKey(key) == true)
			{
				int oldSlotIdx = slotsMap.get(key);
				slots.get(oldSlotIdx).remove(key);
			}
			
			int slotIdx = rng.nextInt(slots.size());
			slots.get(slotIdx).add(key);
			
			slotsMap.put(key, slotIdx);
			
			if(sz > peakOccupancy) { peakOccupancy = cache.size(); }
			
			if(sz >= 4*capacity && sz >= 12) { Errors.warn("[EC (put)] significantly exceeded cache capacity (cache size: " + sz + ", capacity: " + capacity + ")!"); }
		}
		
		return ret;
	}
	public synchronized BlockDataItem lookup(String key) { return cache.get(key); }
	
	public synchronized Set<BlockDataItem> evict(int count, int slotIdx)
	{
		//MiscUtils mu = MiscUtils.getInstance();
		Set<BlockDataItem> ret = new HashSet<>();
		
		List<String> list = slots.get(slotIdx);
		while(list.isEmpty() == false && ret.size() < count)
		{
			String key = list.remove(rng.nextInt(list.size())); 
			BlockDataItem bdi = cache.remove(key);
			Errors.verify(bdi != null);
			ret.add(bdi);
			
			int sidx = slotsMap.remove(key);
			Errors.verify(sidx == slotIdx);
			
			FrameworkHeader header = (FrameworkHeader)bdi.getHeader();
			Errors.verify(header.getKey().equals(key));
		}
		
		return ret;
	}
	
	public EvictionCache(ObjectInputStream is, Random r) throws Exception
	{
		rng = r;
		
		peakOccupancy = 0;
		capacity = is.readInt();
		int size = is.readInt();
		
		if(size > peakOccupancy) { peakOccupancy = size; }
		
		for(int i=0; i<size; i++)
		{
			String key = (String)is.readObject();
			BlockDataItem bdi = new BlockDataItem(is);
			cache.put(key, bdi);
		}
		
		slots = new ArrayList<>();
		for(int slotIdx = 0; slotIdx < capacity; slotIdx++)
		{
			List<String> list = new ArrayList<String>();
			int sz = is.readInt();
			while(list.size() < sz)
			{
				String key = (String) is.readObject();
				list.add(key);
			}
			slots.add(list);
		}
		
		slotsMap = new HashMap<String, Integer>();
		
		int sz = is.readInt();
		for(int i=0; i<sz; i++)
		{
			String key = (String)is.readObject();
			int slotIdx = is.readInt();
			slotsMap.put(key, slotIdx);
		}
	}
	
	public void save(ObjectOutputStream os) throws Exception
	{
		os.writeInt(capacity);
		os.writeInt(cache.size());
		for(String key : cache.keySet())
		{
			os.writeObject(key);
			BlockDataItem bdi = cache.get(key);
			bdi.save(os);
		}
		
		for(List<String> list : slots)
		{
			os.writeInt(list.size());
			for(String key : list) { os.writeObject(key); }
		}
		
		os.writeInt(slotsMap.size());
		for(String key : slotsMap.keySet())
		{
			os.writeObject(key);
			int slotIdx = slotsMap.get(key);
			os.writeInt(slotIdx);
		}
	}

	public int getPeakOccupancy() {	return peakOccupancy; }
}
