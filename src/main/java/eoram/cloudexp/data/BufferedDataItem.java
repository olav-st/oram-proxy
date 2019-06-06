package eoram.cloudexp.data;

import java.nio.ByteBuffer;

/**
 * Implements a data item backed by a {@link ByteBuffer}.
 */
public class BufferedDataItem extends DataItem
{
	protected ByteBuffer buffer = null;
	
	public BufferedDataItem(int size)
	{
		buffer = ByteBuffer.wrap(new byte[size]);
	}
	
	@Override
	public synchronized byte[] getData() { return buffer.array(); }

	public synchronized ByteBuffer getBuffer() { return buffer; }
}
