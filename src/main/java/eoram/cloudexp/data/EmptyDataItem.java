package eoram.cloudexp.data;

import eoram.cloudexp.data.DataItem;

/**
 * Represents an empty data item.
 * <p>
 * <p>
 * Calling {@link getData} on such an item is guaranteed to return null.
 */
public class EmptyDataItem extends DataItem
{
	public EmptyDataItem() {}
	
	@Override
	public synchronized byte[] getData() { return null; }
}
