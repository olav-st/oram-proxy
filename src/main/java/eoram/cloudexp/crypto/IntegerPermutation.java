package eoram.cloudexp.crypto;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Represents an integer permutation.
 */
public interface IntegerPermutation 
{
	public int map(int idx);
	public int reverseMap(int mappedIdx);
	
	public void save(ObjectOutputStream os) throws Exception;
	public void load(ObjectInputStream is) throws Exception;
}
