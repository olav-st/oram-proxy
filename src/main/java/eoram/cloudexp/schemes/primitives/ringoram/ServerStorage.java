package eoram.cloudexp.schemes.primitives.ringoram;
/*
 * store each bucket to a file
 */

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.crypto.CryptoProvider;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.service.DownloadOperation;
import eoram.cloudexp.service.ScheduledOperation;
import eoram.cloudexp.service.UploadOperation;
import eoram.cloudexp.utils.Errors;

public class ServerStorage {
	private Log log = Log.getInstance();

	private ExternalStorageInterface si;
	private CryptoProvider cp;
	private BucketMetadata[] metadataCache;
	private ByteSerialize seria;

	public ServerStorage(ExternalStorageInterface si, CryptoProvider cp, boolean useMetadataCaching)
	{
		this.si = si;
		this.cp = cp;
		if(useMetadataCaching)
		{
			this.metadataCache = new BucketMetadata[Configs.BUCKET_COUNT];
		}
		this.seria = new ByteSerialize();
	}

	public byte[] get_block_data(long reqId, int pathId, int[] read_offset, BucketMetadata[] meta_list)
	{
		log.append("get_block_data reqId: " + reqId + " pathId: " + pathId, Log.TRACE);
		byte[] data = new byte[Configs.BLOCK_DATA_LEN];//all zero
		//read only one block from every bucket in the path
		for (int pos = pathId, i = 0;pos>=0;pos = (pos - 1) >> 1, i++)
		{
			//Read block
			String key = pos + "";
			int offset = 4 + read_offset[i] * Configs.BLOCK_DATA_LEN; //To skip id and other blocks
			int numBytes = Configs.BLOCK_DATA_LEN;
			DownloadOperation op = new DownloadOperation(reqId, key, offset, numBytes);
			ScheduledOperation sop = si.downloadObject(op);

			sop.waitUntilReady();
			if(sop.wasSuccessful() != true)
			{
				Errors.error("get_block operation failed");
			}

			//Update metadata: can't read this block before next shuffle
			BucketMetadata meta = get_metadata(reqId, pos);
			meta.set_meta_validbit(read_offset[i]);
			meta.add_meta_readcounter();
			set_metadata(reqId, pos, meta);//update storage

			//XOR in the data of this block
			byte[] block_data = sop.getDataItem().getData();
			for(int j=0;j<Configs.BLOCK_DATA_LEN;j++){
				data[j] ^= block_data[j];//exclusive or
			}

			if(pos == 0)
				break;
		}
		return data;
	}

	public BucketMetadata get_metadata(long reqId, int bucketId)
	{
		log.append("get_metadata reqId: " + reqId + " bucketId: " + bucketId, Log.TRACE);
		if(metadataCache != null && metadataCache[bucketId] != null)
		{
			return metadataCache[bucketId];
		}
		else
		{
			String key = bucketId + "_meta";

			DownloadOperation op = new DownloadOperation(reqId, key);
			ScheduledOperation sop = si.downloadObject(op);

			byte[] data = new byte[Configs.METADATA_BYTES_LEN];
			sop.waitUntilReady();
			if(sop.wasSuccessful() == true)
			{
				data = sop.getDataItem().getData();
			}
			else {
				Errors.error("get_metadata operation failed");
			}
			byte[] decrypted = cp.decrypt(data);
			BucketMetadata meta = seria.metadataFromSerialize(decrypted);

			//Update cache
			if(metadataCache != null)
			{
				metadataCache[bucketId] = meta;
			}

			return meta;
		}
	}

	public void set_metadata(long reqId, int bucketId, BucketMetadata meta)
	{
		log.append("set_metadata reqId: " + reqId + " bucketId: " + bucketId, Log.TRACE);
		String key = bucketId + "_meta";

		byte[] data = seria.metadataSerialize(meta);
		byte[] encrypted = cp.encrypt(data);
		UploadOperation op = new UploadOperation(reqId, key, new SimpleDataItem(encrypted));
		ScheduledOperation sop = si.uploadObject(op);

		sop.waitUntilReady();
		if(sop.wasSuccessful() != true)
		{
			Errors.error("set_bucket operation failed");
		}
		//Update cache
		if(metadataCache != null)
		{
			metadataCache[bucketId] = meta;
		}
	}

	//Download bucket metdata
	public BucketMetadata[] get_metadata_along_path(long reqId, int pathId)
	{
		log.append("get_metadata_along_path reqId: " + reqId + " pathId: " + pathId, Log.TRACE);
		//get bucket meta data from storage
		BucketMetadata[] meta_list = new BucketMetadata[Configs.HEIGHT];
		int index = 0;
		for(int pos_run = pathId; pos_run >= 0; pos_run = (pos_run - 1) >> 1)
		{
			meta_list[index] = get_metadata(reqId, pos_run);
			index++;

			if(pos_run == 0)
				break;
		}

		return meta_list;
	}

	//download bucket from server
	public Bucket get_bucket(long reqId, int bucketId)
	{
		log.append("get_bucket reqId: " + reqId + " bucketId: " + bucketId, Log.TRACE);
		String key = bucketId + "";

		DownloadOperation op = new DownloadOperation(reqId, key);
		ScheduledOperation sop = si.downloadObject(op);

		byte[] data = new byte[Configs.METADATA_BYTES_LEN];
		sop.waitUntilReady();
		if(sop.wasSuccessful() == true)
		{
			data = sop.getDataItem().getData();
		}
		else {
			Errors.error("get_bucket operation failed");
		}
		return seria.bucketFromSerialize(data);
	}
	
	//upload bucket to server
	public void set_bucket(long reqId, int bucketId, Bucket bucket)
	{
		log.append("set_bucket reqId: " + reqId + " bucketId: " + bucketId, Log.TRACE);
		String key = bucketId + "";

		UploadOperation op = new UploadOperation(reqId, key, new SimpleDataItem(seria.bucketSerialize(bucket)));
		ScheduledOperation sop = si.uploadObject(op);

		sop.waitUntilReady();
		if(sop.wasSuccessful() != true)
		{
			Errors.error("set_bucket operation failed");
		}
	}
}