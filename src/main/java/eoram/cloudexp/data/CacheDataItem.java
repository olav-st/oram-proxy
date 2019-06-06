package eoram.cloudexp.data;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eoram.cloudexp.utils.Errors;

/**
 * Represents a data item in a cache, which may or may not be encrypted.
 *
 */
public class CacheDataItem extends DataItem 
{
	protected PendingOpDataItem podi = null;
	protected EncryptedDataItem edi = null;
	
	protected BlockDataItem bdi = null;
	
	
	public CacheDataItem(PendingOpDataItem d) 
	{
		podi = d; edi = new EncryptedDataItem(podi); bdi = null;
	}
	
	public CacheDataItem(EncryptedDataItem d) 
	{
		podi = null; edi = d; bdi = null;
	}
	
	public CacheDataItem(BlockDataItem b) 
	{
		podi = null; edi = null; bdi = b;
	}
	
	public CacheDataItem(ObjectInputStream is) throws Exception
	{
		podi = null; edi = null; bdi = new BlockDataItem(is);
	}
	
	@Override
	public synchronized byte[] getData() 
	{
		return null;
	}
	
	protected synchronized void check()
	{
		if((bdi == null && edi == null) || (bdi != null && edi != null)) { Errors.error("Coding FAIL!") ; }
	}
	
	public synchronized void ensureOpened()
	{
		check();
		if(bdi == null)
		{
			bdi = new BlockDataItem(edi.getHeader(), edi.getPayload());
			edi = null;
		}
	}
	
	public synchronized boolean isClosed() 
	{
		if(edi != null && (podi != null && podi.isReady() == false)) { return true; }
		
		ensureOpened();
		return false;
	}
	
	public synchronized BlockDataItem getBlock() { ensureOpened(); return bdi; }
	
	public synchronized DataItem getDecryptedDataItem()
	{
		if(isClosed() == true) { return edi; }
		else { return new SimpleDataItem(bdi.getPayload()); }
	}
	
	public synchronized void save(ObjectOutputStream os) throws Exception
	{
		ensureOpened();
		bdi.save(os);
	}
}
