package eoram.cloudexp.service;

/**
 * Represents a list storage operation.
 */
public class ListOperation extends Operation 
{
	public ListOperation(long r) { super(r, "list"); }

	@Override
	public OperationType getType() { return OperationType.LIST; }
}
