package eoram.cloudexp.data;

import java.util.Base64;

/**
 * Represents a simple data item consisting of a byte array.
 */
public class SimpleDataItem extends DataItem 
{
	protected byte[] data = null;
	
	public SimpleDataItem(byte[] d) { data = d; }
	
	public SimpleDataItem(String fromString)
	{
		data = Base64.getDecoder().decode(fromString);
	}

	public synchronized void setData(byte[] d) { data = d; }

	@Override
	public synchronized byte[] getData() { return data; }
}
