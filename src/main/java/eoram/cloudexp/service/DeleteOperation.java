package eoram.cloudexp.service;

/** 
 * Represents a delete storage operation.
 */
public class DeleteOperation extends Operation 
{
	public DeleteOperation(long r, String k) { super(r, k); }
	
	@Override
	public OperationType getType() { return OperationType.DELETE; }
}