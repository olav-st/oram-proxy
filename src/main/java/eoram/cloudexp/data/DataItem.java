package eoram.cloudexp.data;

import java.util.Base64;

/**
 * Represents an abstract data item.
 */
public abstract class DataItem 
{
	public abstract byte[] getData();

	public long getNumBytes()
	{
		return getData().length;
	}
	
	@Override
	public synchronized String toString()
	{
		return Base64.getEncoder().encodeToString(getData());
	}
}
