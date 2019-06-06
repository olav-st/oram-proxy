package eoram.cloudexp.data.encoding;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import eoram.cloudexp.utils.Errors;

/**
 * Implements the block header for ObliviStore.
 * This header include the block id, as an integer.
 */
public class ObliviStoreHeader extends Header 
{
	public static final int encodedByteSize = 4; // 32-bit integer	
	
	protected int blockId = 0;
	
	public ObliviStoreHeader(int bid) { blockId = bid; }
	
	public ObliviStoreHeader(byte[] header) { parse(header); }
	
	public int getBlockId() { return blockId; }
	
	public String toString() { return "" + getBlockId(); }
	
	@Override
	public int encodedByteSize() 
	{
		return encodedByteSize;
	}

	@Override
	public byte[] getEncoding() 
	{
		return ByteBuffer.allocate(encodedByteSize).order(ByteOrder.LITTLE_ENDIAN).putInt(blockId).array();
	}

	@Override
	protected void parse(byte[] header) 
	{
		Errors.verify(header != null && header.length == encodedByteSize);
		blockId = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}

	@Override
	protected Header create(byte[] headerBytes) 
	{
		return new ObliviStoreHeader(headerBytes);
	}
}
