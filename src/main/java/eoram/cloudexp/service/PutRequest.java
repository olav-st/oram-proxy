package eoram.cloudexp.service;

import eoram.cloudexp.data.*;

/**
 * Represents a put request.
 */
public class PutRequest extends Request 
{
	protected DataItem val = null;
	
	public PutRequest(String k, DataItem v) { super(k); val = v; }
	
	PutRequest(long rid, String k, DataItem v) { super(rid, k); val = v; }

	@Override
	public RequestType getType() { return RequestType.PUT; }

	public DataItem getValue() { return val; }
	
	public void setValue(DataItem di) { val = di; }

	@Override
	public String toString() 
	{
		String ret = super.toString();
		ret += ", " + getValue().toString();
		
		return ret;
	}	
}
