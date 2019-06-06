package eoram.cloudexp.service;

import eoram.cloudexp.data.DataItem;

/** 
 * Represents an upload storage operation.
 */
public class UploadOperation extends Operation 
{
	protected DataItem dataItem = null;
	
	public UploadOperation(long r, String k, DataItem di) { super(r, k); dataItem = di; }

	@Override
	public OperationType getType() { return OperationType.UPLOAD; }
	
	public DataItem getDataItem() { return dataItem; }
}
