package eoram.cloudexp.data.encoding;

import eoram.cloudexp.utils.EncodingUtils;
import eoram.cloudexp.utils.Errors;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Implements the block header for the framework's clients.
 * This header includes (a hash of) the block id, but also a version id.
 */
public class FrameworkHeader extends Header 
{
	public static final int encodedHexHashKeyByteSize = 10;	
	protected String hexHashKey = null;
	
	public static final int encodedVersionByteSize = 4;	
	protected int versionId = 0;
	
	public FrameworkHeader() { }
	
	public FrameworkHeader(String h, int v) 
	{
		hexHashKey = h; 
		Errors.verify(hexHashKey.length() == 2 * encodedHexHashKeyByteSize); 
		
		versionId = v;
	}
	public FrameworkHeader(byte[] h) { parse(h); }
	
	
	@Override
	public int encodedByteSize() { return encodedHexHashKeyByteSize + encodedVersionByteSize; }

	@Override
	public byte[] getEncoding() 
	{
		Errors.verify(hexHashKey != null);
		byte[] h = EncodingUtils.getInstance().fromHexString(hexHashKey);
		Errors.verify(h != null && h.length == encodedHexHashKeyByteSize);
		
		ByteBuffer bb = ByteBuffer.allocate(encodedByteSize());
		bb = bb.put(h).order(ByteOrder.LITTLE_ENDIAN).putInt(versionId);
		
		return bb.array();
	}

	@Override
	protected void parse(byte[] header) 
	{
		Errors.verify(header != null);
		byte[] hash = new byte[encodedHexHashKeyByteSize];
		System.arraycopy(header, 0, hash, 0, encodedHexHashKeyByteSize);
		hexHashKey = EncodingUtils.getInstance().toHexString(hash);
		Errors.verify(hexHashKey != null);
		
		byte[] v = new byte[encodedVersionByteSize]; 
		System.arraycopy(header, encodedHexHashKeyByteSize, v, 0, encodedVersionByteSize);
		versionId = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}

	@Override
	protected Header create(byte[] headerBytes) { return new FrameworkHeader(headerBytes); }

	public String getKey() { return hexHashKey; }
	public int getVersionId() { return versionId; }
	
	public String toString() { return "(" + getKey() + ", " + getVersionId() + ")"; }
}
