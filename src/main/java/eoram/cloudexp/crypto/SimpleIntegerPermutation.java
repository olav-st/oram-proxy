package eoram.cloudexp.crypto;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

import eoram.cloudexp.utils.Errors;

/**
 * Simple (array-based) integer permutation.
 */
public class SimpleIntegerPermutation implements IntegerPermutation 
{
	protected int[] map = null;
	protected int[] reverseMap = null;
	
	public SimpleIntegerPermutation(int size, Random rng)
	{
		Errors.verify(size > 0);
		map = new int[size];
		
		for(int i=0; i<size; i++) { map[i] = i; }
		
		for(int i=0; i<size; i++)
		{
			int j = rng.nextInt(size - i)+i;
			int temp = map[j]; map[j] = map[i]; map[i] = temp; // swap
		}
		
		computeReverseMap();
	}
	
	private void computeReverseMap()
	{
		reverseMap = new int[map.length];
		for(int i=0; i<map.length; i++) { reverseMap[map[i]] = i; }
	}
	
	@Override
	public int map(int idx) 
	{
		Errors.verify(idx >= 0 && idx < map.length);
		return map[idx];
	}

	@Override
	public int reverseMap(int mappedIdx) 
	{
		Errors.verify(mappedIdx >= 0 && mappedIdx < map.length);
		return reverseMap[mappedIdx];
	}

	@Override
	public void save(ObjectOutputStream os) throws Exception 
	{
		os.writeInt(map.length);
		for(int i=0; i<map.length; i++) { os.writeInt(map[i]); }
	}

	@Override
	public void load(ObjectInputStream is) throws Exception 
	{
		int size = is.readInt();
		Errors.verify(size > 0);
		
		map = new int[size];
		for(int i=0; i<map.length; i++) { map[i] = is.readInt(); }
		
		computeReverseMap();
	}
}
