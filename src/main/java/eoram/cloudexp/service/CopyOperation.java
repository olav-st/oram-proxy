package eoram.cloudexp.service;

/** 
 * Represents a copy storage operation.
 */
public class CopyOperation extends Operation 
{
	protected String destKey = null;
	
	public CopyOperation(long r, String sk, String dk) { super(r, sk); destKey = dk; }
	
	@Override
	public OperationType getType() { return OperationType.COPY; }
	
	public String getDestinationKey() { return destKey; }

	public String getSourceKey() { return getKey(); }
}
