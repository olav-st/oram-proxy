package eoram.cloudexp.service;

import java.util.concurrent.atomic.AtomicInteger;

/** 
 * Represents a "logical" request.
 * <p><p>
 * A logical request is a named collection of (non-logical) requests.
 * <p>
 * @see Request
 */
public final class LogicalRequest 
{
	private static AtomicInteger nextTag = new AtomicInteger(0);
	
	protected int tag = -1;
	protected String name = null;
	protected int byteSize = -1;
	protected int reqsCount = 0;
	protected double ratio = -1.0;
	
	public LogicalRequest(String n, int bsz, int c)
	{
		tag = nextTag.getAndIncrement();
		name = n;
		byteSize = bsz;
		reqsCount = c;
	}

	public int getReqsCount() { return reqsCount; }
	
	public String getName() { return name; }

	public int getByteSize() { return byteSize; }
	public double getRatio() { return ratio; }
	
	public void setRatio(double r) { ratio = r; }

	
}
