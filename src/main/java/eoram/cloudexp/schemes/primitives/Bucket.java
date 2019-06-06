package eoram.cloudexp.schemes.primitives;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.data.BlockDataItem;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.encoding.FrameworkHeader;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.utils.EncodingUtils;
import eoram.cloudexp.utils.Errors;

/**
 * Represents a bucket (i.e., a collection of one or more {@code BlockDataItem}).
 */
public class Bucket
{
	protected CryptoProvider cp = CryptoProvider.getInstance();
	protected EncodingUtils eu = EncodingUtils.getInstance();
	protected ClientParameters clientParams = ClientParameters.getInstance();
	
	protected DummyFactory df = null;
	
	protected int numBlocks = 0;
	protected List<BlockDataItem> blocks = new ArrayList<>();
	
	protected byte[] encryptedData = null;
	
	public Bucket(DummyFactory df, int b, final DataItem di)
	{
		this(df, b);
		
		byte[] encryptedData = di.getData();
		
		byte[] d = cp.decrypt(encryptedData);
		
		final int blockSize = clientParams.contentByteSize + Header.getByteSize();
		for(int idx = 0; idx < numBlocks; idx++)
		{
			byte[] blockData = new byte[blockSize];
			System.arraycopy(d, idx * blockSize, blockData, 0, blockSize);
			
			// now decode the data
			Map.Entry<Header, byte[]> decoded = eu.decodeBlock(blockData); 
			
			FrameworkHeader header = (FrameworkHeader)decoded.getKey();
			byte[] payload = decoded.getValue(); 
			
			if(df.isDummy(header.getKey()) == false)
			{
				blocks.add(new BlockDataItem(header, payload));
			}
		}
	}
	
	public Bucket(DummyFactory df, int b) { this.df = df; numBlocks = b; blocks.clear(); encryptedData = null; }

	protected void encrypt()
	{
		final int blockSize = clientParams.contentByteSize + Header.getByteSize();
		final int bucketSize = numBlocks * blockSize;
		
		Errors.verify(blocks.size() <= numBlocks);
		for(int idx = blocks.size(); idx < numBlocks; idx++) { blocks.add(df.create()); }
		
		byte[] data = new byte[bucketSize];
		for(int idx = 0; idx < numBlocks; idx++)
		{
			BlockDataItem bdi = blocks.get(idx);
			
			byte[] blockData = eu.encodeBlock(bdi.getHeader(), bdi.getPayload());
			System.arraycopy(blockData, 0, data, idx * blockSize, blockSize);
		}
		encryptedData = cp.encrypt(data);
	}
	
	public DataItem getEncryption() 
	{
		if(encryptedData == null) { encrypt(); }
		return new SimpleDataItem(encryptedData);
	}
	
	public List<BlockDataItem> getBlocks() { return blocks; }
	
	public void clear() { blocks.clear(); }
	
	public void add(BlockDataItem bdi)
	{
		blocks.add(bdi);
		Errors.verify(bdi != null && blocks.size() <= numBlocks);
	}
	
	public void remove(BlockDataItem bdi) { blocks.remove(bdi); }

	public boolean isFull() { return (blocks.size() >= numBlocks); }
}