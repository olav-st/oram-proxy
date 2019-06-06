package eoram.cloudexp.data;

import java.util.Map;

import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.utils.EncodingUtils;
import eoram.cloudexp.utils.Errors;

/**
 * Represents an encrypted data item.
 */
public class EncryptedDataItem extends DataItem 
{
	protected CryptoProvider cp = CryptoProvider.getInstance();
	
	protected DataItem source = null;
	protected byte[] payload = null;
	protected Header header = null;
	
	public EncryptedDataItem(DataItem src) { source = src; payload = null; header = null; }
	
	protected void decrypt()
	{
		if(source == null) { Errors.error("Unsupported use!"); }
		
		byte[] d = cp.decrypt(source.getData());
			
		// now decode the data
		Map.Entry<Header, byte[]> decoded = EncodingUtils.getInstance().decodeBlock(d); 
			
		payload = decoded.getValue(); header = decoded.getKey();
	}
	
	/** Returns the payload **/
	@Override
	public synchronized byte[] getData()
	{
		if(payload == null) { decrypt(); assert(header != null && payload !=null); }
		return payload;
	}
	
	public synchronized byte[] getPayload() { return getData(); }
	
	public synchronized Header getHeader() 
	{
		if(header == null) { decrypt(); assert(header != null && payload !=null); }
		return header;
	}
}
