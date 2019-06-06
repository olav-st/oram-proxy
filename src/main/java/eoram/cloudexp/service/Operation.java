package eoram.cloudexp.service;

import java.util.concurrent.atomic.AtomicLong;

/** 
 * Represents an abstract storage operation.
 * <p><p>
 * Each operation has a unique operation id, and is associated with a single request (through the request id).
 */
public abstract class Operation
{
	public enum OperationType { DOWNLOAD, UPLOAD, DELETE, COPY, LIST };
	
	private static AtomicLong nextOpId = new AtomicLong(1);
	
	protected long reqId = 0;
	protected long opId = 0;
	
	protected String key = null;
	
	public Operation(long r, String k)
	{
		opId = nextOpId.getAndIncrement();
		reqId = r;
		key = k;
	}
	
	public String getKey() { return key; }
	public long getRequestId() { return reqId; }
	public long getOperationId() { return opId; }
	
	public abstract OperationType getType();
}
