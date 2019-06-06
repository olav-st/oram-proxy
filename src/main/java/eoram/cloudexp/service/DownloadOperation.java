package eoram.cloudexp.service;

/** 
 * Represents a download storage operation.
 */
public class DownloadOperation extends Operation 
{
	protected int offset, numBytes;
	protected byte[] data = null;
	public DownloadOperation(long r, String k) {
		super(r, k);
		offset = 0;
		numBytes = -1;
	}

	public DownloadOperation(long r, String k, int off, int nb)
	{
		super(r, k);
		offset = off;
		numBytes = nb;
	}

	protected DownloadOperation(long r, long o, String k) // constructor (unsafe)
	{
		super(r, k);
		opId = o;
	}

	public void setData(byte[] d) { data = d; }

	@Override
	public OperationType getType() { return OperationType.DOWNLOAD; }

	public int getOffset() { return offset; }
	public int getNumBytes() { return numBytes; }

}
