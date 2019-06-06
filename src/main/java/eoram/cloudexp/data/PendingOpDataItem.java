package eoram.cloudexp.data;

import eoram.cloudexp.service.ScheduledOperation;

/**
 * Represents a data item which is the result of a pending operation. 
 * The data becomes available as soon as the (pending) operation completes.
 *
 */
public class PendingOpDataItem extends DataItem
{
	protected ScheduledOperation sop = null;
	
	public PendingOpDataItem(ScheduledOperation s) 
	{
		sop = s;
	}
	
	@Override
	public synchronized byte[] getData() 
	{
		return sop.getDataItem().getData();
	}

	public synchronized boolean isReady() { return sop.isReady(); }
}
