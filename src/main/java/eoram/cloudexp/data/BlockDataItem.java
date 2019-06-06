package eoram.cloudexp.data;

import java.io.ObjectInputStream;

import java.io.ObjectOutputStream;

import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.utils.Errors;

/**
 * Implements a block data item (unencrypted).
 */
public class BlockDataItem extends DataItem 
{
	protected CryptoProvider cp = CryptoProvider.getInstance();
	
	protected byte[] payload = null;
	protected Header header = null;
	
	public BlockDataItem(Header h, byte[] p) { payload = p; header = h; }
	
	public BlockDataItem(ObjectInputStream is) throws Exception
	{
		header = Header.load(is);
		
		int length = is.readInt(); 
		if(length <= 0) { Errors.error("Coding FAIL!"); }
		
		payload = new byte[length];
		is.readFully(payload);
	}

	/** Returns encrypted data **/
	@Override
	public synchronized byte[] getData() { return cp.encryptBlock(header, payload); }
	
	/** 
	 * @return a data item consisting of an encryption of this block. 
	 */
	public DataItem getEncryption() { return new SimpleDataItem(getData()); }
	
	public synchronized void setHeader(Header h) { header = h; }
	public synchronized void setPayload(byte[] d)
	{
		if(d == null) { Errors.error("Unsupported use!"); }
		payload = d;
	}
	
	public synchronized Header getHeader() { return header; }
	public synchronized byte[] getPayload() { return payload; }
	
	public synchronized void save(ObjectOutputStream os) throws Exception
	{
		Header.save(os, header);
		os.writeInt(payload.length);
		os.write(payload);
	}

	public BlockDataItem copy() { return new BlockDataItem(header, payload); }
}
