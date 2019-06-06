package eoram.cloudexp.utils;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.data.encoding.Header;

import java.util.AbstractMap;
import java.util.Formatter;
import java.util.Map;

/**
 * Implements encoding utility functions.
 */
public class EncodingUtils 
{
	private static final EncodingUtils instance = new EncodingUtils();
	
	private EncodingUtils() {}
	
	public static EncodingUtils getInstance() { return instance; }
	
	private ClientParameters clientParams = ClientParameters.getInstance();
	
	
	public byte[] encodeBlock(Header header, byte[] objectPayload)
	{
		int headerByteSize = header.encodedByteSize();
		int encodedByteSize = headerByteSize + objectPayload.length;
		
		byte[] ret = new byte[encodedByteSize];
		
		byte[] headerBytes = header.getEncoding();
		
		System.arraycopy(headerBytes, 0, ret, 0, headerByteSize);
		System.arraycopy(objectPayload, 0, ret, headerByteSize, objectPayload.length);
		
		// note: the first rByteSize bytes of data are zeros!
		
		return ret;
	}
	
	public Map.Entry<Header, byte[]> decodeBlock(byte[] encodedData)
	{
		int headerByteSize = Header.getByteSize();
	
		byte[] headerBytes = new byte[headerByteSize];
		System.arraycopy(encodedData, 0, headerBytes, 0, headerByteSize);
		
		Header header = Header.parseHeader(headerBytes);
		
		int payloadByteSize = encodedData.length - headerByteSize;
		byte[] objectPayload = new byte[payloadByteSize];
		System.arraycopy(encodedData, headerByteSize, objectPayload, 0, objectPayload.length);
	
		return new AbstractMap.SimpleEntry<Header, byte[]>(header, objectPayload);
	}

	public byte[] fromHexString(String str)
	{
		/* From: https://stackoverflow.com/questions/140131/convert-a-string-representation-of-a-hex-dump-to-a-byte-array-using-java */
		int len = str.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(str.charAt(i), 16) << 4)
					+ Character.digit(str.charAt(i+1), 16));
		}
		return data;
	}

	public String toHexString(byte[] data)
	{
		Formatter formatter = new Formatter();
		for (byte b : data) {
			formatter.format("%02X", b);
		}
		return formatter.toString();
	}

	public byte[] getDummyBytes(int size) { return new byte[size]; }
}
