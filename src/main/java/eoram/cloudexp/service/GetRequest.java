package eoram.cloudexp.service;

/**
 * Represents a get request.
 *
 */
public class GetRequest extends Request 
{
	private Integer start, end;

	public GetRequest(String k) { super(k); }
	public GetRequest(String k, int start, int end) {
		super(k);
		this.start = start;
		this.end = end;
	}

	// default visibility
	GetRequest(long rid, String k) { super(rid, k); }

	@Override
	public RequestType getType() { return RequestType.GET; }
	
	
	@Override
	public String toString() 
	{
		String ret = super.toString();
		return ret;
	}

	public Integer getStartOffset() {
		return start;
	}

	public Integer getEndOffset() {
		return end;
	}
}
