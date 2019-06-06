package eoram.cloudexp.schemes.primitives;

import java.io.ObjectOutputStream;
import java.util.Random;
import java.util.Set;

/**
 * Defines the interface for the (CURIOUS) Framework Client's subORAM (aka partition ORAM or small ORAMs) modules. 
 * <p><p>
 * The main function each subORAM module must implement is {@code getReadRemoveWriteInfo}.
 */
public interface FrameworkSubORAM 
{
	public class ReadRemoveWriteInfo
	{
		public Set<Integer> readSet = null;
		public Set<Integer> removeSet = null;
		public Set<Integer> writeSet = null;
	};
	
	public int getIdx();
	
	public int getDummyPos();
	public boolean isDummyPos(int pos);
	
	public void acquireOwnership();
	public void releaseOwnership();
	
	public ReadRemoveWriteInfo getReadRemoveWriteInfo(int pos, Random rng);
	
	public String getName();
	
	public int[] getReadWriteCounts();
	
	public void save(ObjectOutputStream os) throws Exception;
}
