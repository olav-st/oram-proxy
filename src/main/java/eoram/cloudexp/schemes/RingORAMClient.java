package eoram.cloudexp.schemes;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.implementation.AbstractClient;
import eoram.cloudexp.schemes.primitives.ringoram.*;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledRequest;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Implements RingORAM (see Ren et al. "Constants Count: Practical Improvements to Oblivious RAM").
 * <p><p>
 * This implementation is based on https://github.com/caohuikang/RingORAM
 * <p>
 *
 */

public class RingORAMClient extends AbstractClient
{
	private int evict_count;
	private int evict_g;
	private int[] position_map;
	private boolean useMetadataCaching;
	
	Stash stash;
	MathUtility math;
	ServerStorage storage;
	ByteSerialize seria;
	
	@SuppressWarnings("rawtypes")
	public RingORAMClient(boolean useMetadataCaching)
	{
		//Calculate configs
		Configs.BLOCK_DATA_LEN = ClientParameters.getInstance().contentByteSize + ClientParameters.getInstance().randomPrefixByteSize;
		Configs.BUCKET_COUNT = (int) Math.pow(2, Math.ceil(Math.log(ClientParameters.getInstance().maxBlocks / Configs.REAL_BLOCK_COUNT)/Math.log(2))) - 1;
		Configs.BLOCK_COUNT = Configs.BUCKET_COUNT * Configs.REAL_BLOCK_COUNT;
		Configs.HEIGHT = (int) (Math.log(Configs.BUCKET_COUNT)/Math.log(2) + 1);
		Configs.LEAF_COUNT = (Configs.BUCKET_COUNT +1)/2;
		Configs.LEAF_START = Configs.BUCKET_COUNT - Configs.LEAF_COUNT;
		//Setup
		this.evict_count = 0;
		this.evict_g = 0;
		this.position_map = new int[Configs.BLOCK_COUNT];
		this.useMetadataCaching = useMetadataCaching;
		this.stash = new Stash();
		this.math = new MathUtility();
		this.seria = new ByteSerialize();
		//when initializing, assign all blocks a random path id
		for(int i = 0; i< Configs.BLOCK_COUNT; i++){
			this.position_map[i] = math.getRandomLeaf() + Configs.LEAF_START;
		}
	}

	public RingORAMClient()
	{
		this(false);
	}

	@Override
	protected void init(boolean reset)
	{
		//Initialize the serverside storage
		this.storage = new ServerStorage(s, cp, useMetadataCaching);
		for(int i = 0; i< Configs.BUCKET_COUNT; i++){
			Bucket bucket = new Bucket();
			BucketMetadata meta = new BucketMetadata();
			meta.init_block_index();
			meta.set_offset(math.get_random_permutation(Configs.Z));
			storage.set_bucket(Request.initReqId, i, bucket);
			storage.set_metadata(Request.initReqId, i, meta);
		}
	}

	@Override
	public ScheduledRequest scheduleGet(GetRequest req)
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		//try
		{
			byte[] data = oblivious_access(req.getId(), Integer.parseInt(req.getKey()), Configs.OPERATION.ORAM_ACCESS_READ, null);
			byte[] decrypted = cp.decrypt(data);
			sreq.onSuccess(new SimpleDataItem(decrypted));
		}
		//catch (Exception e) { sreq.onFailure(); }
		return sreq;
	}

	@Override
	public ScheduledRequest schedulePut(PutRequest req)
	{
		ScheduledRequest sreq = new ScheduledRequest(req);
		//try
		{
			byte[] encrypted = cp.encrypt(req.getValue().getData());
			oblivious_access(req.getId(), Integer.parseInt(req.getKey()), Configs.OPERATION.ORAM_ACCESS_WRITE, encrypted);
			sreq.onSuccess(new EmptyDataItem());
		}
		//catch (Exception e) { sreq.onFailure(); }
		return sreq;
	}

	/* core operation in ring oram
	 * @param blockIndex: block unique index
	 * @param op: request operation, read or write
	 * @param newdata: when operation is write, newdata is the data that you want to write
	 */
	public byte[] oblivious_access(long reqId, int blockIndex, Configs.OPERATION op, byte[] newdata){
		log.append("Process request " + reqId, Log.TRACE);

		byte[] readData = null;//return data

		//get leaf id and update a new random leaf id
		int position = position_map[blockIndex];
		int position_new = math.getRandomLeaf() + Configs.LEAF_START;
		position_map[blockIndex] = position_new;

		//read block from server, and insert into the stash
		read_path(reqId, position, blockIndex);
		//find block from the stash
		Block block = stash.find_by_blockIndex(blockIndex);

		if(op == Configs.OPERATION.ORAM_ACCESS_WRITE){
			if(block==null){//not in the stash
				//log.append("when write, can't find block in the stash");
				//new block and add it to the stash
				block = new Block(blockIndex,position_new,newdata);
				stash.add(block);
			}else{//find in the stash, update stash block
				block.setData(newdata);
				block.setLeaf_id(position_new);
			/*	if(stash.find_by_address(blockIndex)==block)
					log.append("find block in stash and update successful!");
				else
					log.append("find block in the stash and update fail!");*/
			}
			readData = block.getData();
		}
		if(op == Configs.OPERATION.ORAM_ACCESS_READ){
			if(block != null){//find block in the stash or servere
				log.append("when read block "+blockIndex+" find block in the stash.", Log.TRACE);
				readData = block.getData();
			}
		}

		evict_count = (evict_count+1)%Configs.SHUFFLE_RATE;
		//evict count reaches shuffle rate, evict path
		if(evict_count == 0){
			evict_path(reqId, math.gen_reverse_lexicographic(evict_g, Configs.BUCKET_COUNT, Configs.HEIGHT));
			evict_g = (evict_g+1)% Configs.LEAF_COUNT;
		}

		//early re-shuffle current path
		BucketMetadata[] meta_list = get_metadata(reqId, position);
		early_reshuffle(reqId, position, meta_list);

		return readData;
	}

	public void read_path(long reqId, int pathID, int blockIndex) {
		//get meta data of the buckets in the path
		BucketMetadata[] meta_list = get_metadata(reqId, pathID);
		//read proper block in the path
		read_block(reqId, pathID, blockIndex,meta_list);
	}

	public BucketMetadata[] get_metadata(long reqId, int pathID)
	{
		return storage.get_metadata_along_path(reqId, pathID);
	}

	public void read_block(long reqId, int pathID,int blockIndex,BucketMetadata[] meta_list){

		boolean found = false;// record if the block is in the path
		//offset in the bucket data of the block that will be read from the server
		int[] read_offset = new int[Configs.HEIGHT];

		for (int i = 0, pos_run = pathID;pos_run>=0; pos_run = (pos_run - 1) >> 1, i++) {
			if(found){//if found the block, then read a dummy block
				read_offset[i] = math.get_random_dummy(meta_list[i].getValid_bits(), meta_list[i].get_offset());
			}else{//not found the block
				for(int j=0;j<Configs.REAL_BLOCK_COUNT;j++){
					int offset = meta_list[i].get_offset()[j];
					if((meta_list[i].get_block_index()[j]==blockIndex) &&
							(meta_list[i].getValid_bits()[offset]==1)){//block is in this bucket
						read_offset[i] = offset;
						found = true;
					}
					if(found)
						break;
				}
				if(!found){//block is not in this bucket
					read_offset[i] = math.get_random_dummy(meta_list[i].getValid_bits(), meta_list[i].get_offset());
				}
			}
			if(pos_run == 0)
				break;
		}

		//send message to server
		byte[] responseBytes = storage.get_block_data(reqId, pathID, read_offset, meta_list);

		if(found)
		{
			//add to stash
			Block blk = new Block(blockIndex,pathID,responseBytes);
			stash.add(blk);
		}
	}

	public void evict_path(long reqId, int pathID){
		//read path from server
		for (int pos_run = pathID;pos_run>=0;pos_run = (pos_run - 1) >> 1) {
			read_bucket(reqId, pos_run);
			if (pos_run == 0)
				break;
		}
		//write path to server
		for (int pos_run = pathID;pos_run>=0;pos_run = (pos_run - 1) >> 1) {
			write_bucket(reqId, pos_run);
			if (pos_run == 0)
				break;
		}
	}

	public void read_bucket(long reqId, int bucket_id){
		//send request to server
		Bucket bucket = storage.get_bucket(reqId, bucket_id);
		BucketMetadata meta = storage.get_metadata(reqId, bucket_id);

		//recover bucket from responseBytes
		int[] block_index = meta.get_block_index();
		int[] offset = meta.get_offset();
		byte[] valid_bits = meta.getValid_bits();
		for(int i=0;i<Configs.REAL_BLOCK_COUNT;i++){
			//real block and not been accessed before, add to the stash
			if((block_index[i]>=0) && (valid_bits[offset[i]]==(byte)1)){
				byte[] block_data = bucket.getBlock(offset[i]);
				stash.add(new Block(block_index[i],position_map[block_index[i]],block_data));
			}
		}
	}

	public void write_bucket(long reqId, int bucket_id){
		BucketMetadata meta = new BucketMetadata();
		Block[] block_list = new Block[Configs.REAL_BLOCK_COUNT];

		//get the proper block that can place in the bucket to the block_list,
		//count record the real block count
		int count = stash.remove_by_bucket(bucket_id, Configs.REAL_BLOCK_COUNT, block_list);

		//shuffle the block data offset in bucket data
		meta.set_offset(math.get_random_permutation(Configs.Z));
		int[] offset = meta.get_offset();
		byte[] bucket_data = new byte[Configs.Z* Configs.BLOCK_DATA_LEN];
		for(int i=0;i<count;i++){
			int offset_i = offset[i]* Configs.BLOCK_DATA_LEN;
			byte[] block_data = block_list[i].getData();
			for(int j = 0; j< Configs.BLOCK_DATA_LEN; j++){
				bucket_data[offset_i+j] = block_data[j];
			}
			meta.set_blockIndex_bit(i, block_list[i].getBlockIndex());
			position_map[block_list[i].getBlockIndex()] = bucket_id;
		}
		//full fill bucket
		for(int i = count;i<Configs.Z;i++){
			int offset_i = offset[i]* Configs.BLOCK_DATA_LEN;
			for(int j = 0; j< Configs.BLOCK_DATA_LEN; j++){
				bucket_data[offset_i+j] = 0;
			}
			if(i<Configs.REAL_BLOCK_COUNT){//dummy block to fill real block space
				meta.set_blockIndex_bit(i, -1);
			}
		}
		Bucket bucket = new Bucket(bucket_id,bucket_data);
		storage.set_bucket(reqId, bucket_id, bucket);
		storage.set_metadata(reqId, bucket_id, meta);
	}

	public void early_reshuffle(long reqId, int pathID, BucketMetadata[] meta_list){
		//shuffle bucket in the path
		for (int pos_run = pathID, i = 0;pos_run>=0;pos_run = (pos_run - 1) >> 1, i++) {
			if (meta_list[i].getRead_counter() >= (Configs.DUMMY_BLOCK_COUNT-2)) {
				log.append("early reshuffle in pos " +pos_run, Log.TRACE);
				read_bucket(reqId, pos_run);
				write_bucket(reqId, pos_run);
			}
			if (pos_run == 0)
				break;
		}
	}

	@Override
	protected void load(ObjectInputStream is) throws Exception {

	}

	@Override
	protected void save(ObjectOutputStream os) throws Exception {

	}

	@Override
	public boolean isSynchronous() { return true; } //RingORAM is synchronous

	@Override
	public String getName() { return "RingORAM"; }

	@Override
	public long peakByteSize() {
		final double bitsPerByte = 8.0;

		int entryByteSize = clientParams.contentByteSize + Header.getByteSize();
		long stashSize = Configs.STASH_SIZE * entryByteSize;

		long effectiveN = Math.min(clientParams.maxBlocks, clientParams.localPosMapCutoff);

		int logMaxBlocks = (int)Math.ceil(Math.log(effectiveN)/Math.log(2.0));
		int posMapEntrySize = (int)Math.ceil(logMaxBlocks/bitsPerByte);
		long posMapSize = effectiveN * posMapEntrySize;

		return stashSize + posMapSize;
	}
}
