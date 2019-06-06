package eoram.cloudexp.data;

/**
 * Represents a dummy data item.
 *
 */
public class DummyDataItem extends DataItem 
{
	protected byte[] data = null;
	protected int byteSize = 0;
	
	public DummyDataItem(int s) { byteSize = s; data = null; }
	
	@Override
	public synchronized byte[] getData() 
	{
		if(data == null)
		{
			assert(byteSize > 0);
			data = new byte[byteSize];
		}
		
		return data;
	}

}
