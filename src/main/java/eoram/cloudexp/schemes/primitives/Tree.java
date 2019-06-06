package eoram.cloudexp.schemes.primitives;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.implementation.LocalStorage;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.service.*;
import eoram.cloudexp.utils.Errors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;

public class Tree
{
	protected Log log = Log.getInstance();

	SecureRandom rnd;

	public int C;
	public int N; // the number of logic blocks in the tree
	public int D; // depth of the tree
	public int Z; // number of blocks in a bucket
	public int dataLen; // data length in bits

	public ExternalStorageInterface storedTree;
	long treeSize;
	public Stash stash;

	int dataSize; // measured in byte.
	int extDataSize; // measured in byte.

	int stashSize;
	int recLevel;
	boolean stashUseLS;
	int nonceLen; // length of nonce (to encrypt each data piece) in bytes
	byte[] clientKey;

	public Tree(SecureRandom rand, int cParam, int zParam, int rLevel, int sSize, boolean sUseLS, int nLen, byte[] cKey)
	{
		rnd = rand;
		C = cParam;
		Z = zParam;
		recLevel = rLevel;
		stashSize = sSize;
		stashUseLS = sUseLS;
		nonceLen = nLen;
		clientKey = cKey;
	}

	/*
	 * Given input "data" (to be outsourced), initialize the server side storage and return the client side position map.
	 * No recursion on the tree is considered.
	 */
	public BitSet[] initialize(ExternalStorageInterface si, int maxBlocks, int dSize, BitSet[] data)
	{
		storedTree = si;
		dataSize = dSize;
		extDataSize = dataSize + 4 + 4;

		// generate a random permutation for init, so we don't get a silly stash overflow
		List<Integer> permutation = new ArrayList<Integer>();
		for (int i = 0; i < maxBlocks; i++) { permutation.add(i); }
		Collections.shuffle(permutation);

		// TODO
		buildTree(maxBlocks, permutation, data);
		stash = new Stash(stashSize, recLevel, stashUseLS);


		// setup the position map according to the permuted blocks
		BitSet[] posMap = new BitSet[(N + C-1) / C];	// clientPosMap[i] is the leaf label of the i-th block.
		for (int i = 0; i < posMap.length; i++)	{ posMap[i] = new BitSet(C*D); }

		for (int i = 0; i < N; i++)
		{
			int p = i;
			if(i < permutation.size()) { p = permutation.get(i); }
			TreeBasedUtils.writePositionMap(C, posMap, this, i, p);

			//{ log.append("[POB (initialize)] Block " + i + " -> leaf " + p, Log.TRACE); }
		}

		return posMap;
	}

	public void buildTree(int maxBlocks, List<Integer> permutation, BitSet[] dataArray)
	{
//			storedTree = new LocalStorage();
//			storedTree.initialize(null, "/tmp/Cloud" + recLevel);

		SessionState ss = SessionState.getInstance();
		Map<String, Request> fastInitMap = ss.fastInitMap;
		if(ss.fastInit == false) {  fastInitMap = null; }

		// set N to be the smallest power of 2 that is bigger than 'data.length'.
		N = (int) Math.pow(2, Math.ceil(Math.log(maxBlocks)/Math.log(2)));
		D = TreeBasedUtils.bitLength(N)-1;


		final int removeIntervalSize = 512; final double sizeFactorForSlowdown = 0.75;
		final int logIntervalSize = 512;
		Vector<Pollable> v = new Vector<Pollable>();

		// initialize the tree
		treeSize = 2*N-1;
		for (int i = 0; i < treeSize; i++)
		{
			Bucket temp;
			if (i < treeSize/2) { temp = new Bucket(new Block()); }
			else {
				if (i-N+1 < maxBlocks)
				{
					int id = permutation.indexOf(i-N+1); // make sure we are consistent with the permutation
					int label = i-N+1;

					BitSet data = null;

					String blockIdStr = "" + id;
					if(recLevel == 0 && fastInitMap != null && fastInitMap.containsKey(blockIdStr) == true)
					{
						Request req = fastInitMap.get(blockIdStr);
						Errors.verify(req.getType() == Request.RequestType.PUT);
						PutRequest put = (PutRequest)req;
						byte[] val = put.getValue().getData();
						Errors.verify(ClientParameters.getInstance().contentByteSize <= dataSize);
						Errors.verify(val.length <= dataSize);

						data = BitSet.valueOf(val);
					}
					else
					{
						if(dataArray != null)
						{
							Errors.verify(dataArray.length > id);
							data = dataArray[id];
						}
						else
						{
							long[] val = new long[1]; val[0] = id;
							data = BitSet.valueOf(val);
						}
					}

					temp = new Bucket(new Block(data, id, label));

					//{ log.append("[PathORAMBasic (BuildTree)] (R" + recLevel +") putting block " + id + " to label " + label + " (objectKey: " + recLevel + "#" + (i) + ").", Log.TRACE); }
				}
				else
					temp = new Bucket(new Block());
			}

			temp.encryptBlocks();

			String objectKey = recLevel + "#" + (i);
			DataItem di = new SimpleDataItem(temp.toByteArray());
			UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
			ScheduledOperation sop = storedTree.uploadObject(upload);

			v.add(sop);

			if(i > 0 && (i % removeIntervalSize) == 0)
			{
				Pollable.removeCompleted(v);

				if(v.size() >= (int)(removeIntervalSize * sizeFactorForSlowdown))
				{
					{ log.append("[PathORAMBasic (BuildTree)] Slowing down so storage can catch up...", Log.TRACE); }

					int factor = (int)Math.ceil(v.size() / removeIntervalSize); if(factor > 5) { factor = 5; }
					try { Thread.sleep(factor * 5); } catch (InterruptedException e) { Errors.error(e); }
					Pollable.removeCompleted(v);
				}
			}

			if(i > 0 && (i % logIntervalSize) == 0)
			{
				Log.getInstance().append("[PathORAMBasic (BuildTree)] Uploaded " + (i - v.size()) + " nodes so far.", Log.TRACE);
			}
		}

		// This waitForCompletion ensures can be used asynchronously!
		Pollable.waitForCompletion(v);
	}


	public Block[] readBuckets(long reqId, int leafLabel) {

		Bucket[] buckets = getBucketsFromPath(reqId, leafLabel);
		Block[] res = new Block[Z*buckets.length];
		int i = 0;
		for (Bucket bkt : buckets)
		{
			for (Block blk : bkt.blocks)
			{
					/*{ // debug only
						Block blk2 = new Block(blk);
						blk2.dec();
						if(blk2.isDummy() == false) { log.append("[POB (readBuckets)] Found block with id " + blk2.id, Log.TRACE); }
					}*/
				res[i++] = new Block(blk);
			}
		}

		return res;
	}

	private Bucket[] getBucketsFromPath(long reqId, int leaf)
	{
		Bucket[] ret = new Bucket[D+1];

		Vector<ScheduledOperation> v = new Vector<ScheduledOperation>();

		int temp = leaf; //((leaf+1)>>1)-1;
		for (int i = 0; i < ret.length; i++)
		{
			String objectKey = recLevel + "#" + (temp);
			DownloadOperation download = new DownloadOperation(reqId, objectKey);
			ScheduledOperation sop = storedTree.downloadObject(download);
			v.add(sop);

			// debug only
			//{ log.append("[POB (getBucketsFromPath)] reading down to leaf " + leaf + " (" + (leaf - (N-1))  + ") objectKey: " + objectKey, Log.ERROR); }

			if (temp > 0) { temp = ((temp+1)>>1)-1; }
		}

		Pollable.waitForCompletion(v);
		for (int i = 0; i < ret.length; i++) { ret[i] = new Bucket(v.get(i).getDataItem().getData()); }

		return ret;
	}


	public class Block {
		public BitSet data;
		public int id; // range: 0...N-1;
		public int treeLabel;

		public byte[] r;

		public Block(Block blk) {
			assert (blk.data != null) : "no BitSet data pointers is allowed to be null.";
			try { data = (BitSet) blk.data.clone(); }
			catch (Exception e) { e.printStackTrace(); System.exit(1); }
			id = blk.id;
			treeLabel = blk.treeLabel;
			r = blk.r;
		}

		Block(BitSet data, int id, int label) {
			assert (data != null) : "Null BitSet data pointer.";
			this.data = data;
			this.id = id;
			this.treeLabel = label;
		}

		public Block() {
			data = new BitSet(dataSize*8);
			id = N; // id == N marks a dummy block, so the range of id is from 0 to N, both ends inclusive. Hence the bit length of id is D+1.
			treeLabel = 0;
		}

		public Block(byte[] bytes) {
			byte[] bs = new byte[dataSize];
			ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
			bb = bb.get(bs);
			data = BitSet.valueOf(bs);
			id = bb.getInt();
			treeLabel = bb.getInt();
			r = new byte[nonceLen];
			bb.get(r);
		}

		public Block(byte[] bytes, boolean stash) {
			byte[] bs = new byte[dataSize];
			ByteBuffer bb = ByteBuffer.wrap(bytes); //.order(ByteOrder.LITTLE_ENDIAN);
			bb = bb.get(bs);
			data = BitSet.valueOf(bs);
			id = bb.getInt();
			treeLabel = bb.getInt();
		}

		public boolean isDummy() {
			assert (r == null) : "isDummy() was called on encrypted block";
			return id == N;
		}

		public void erase() { id = N; treeLabel = 0; }


		public byte[] toByteArray() {
			ByteBuffer bb = ByteBuffer.allocate(extDataSize+nonceLen);

			// convert data into a byte array of length 'dataSize'
			byte[] d = new byte[dataSize];
			byte[] temp = data.toByteArray();
			for (int i=0; i<temp.length; i++) {
				d[i] = temp[i];
			}

			bb.put(d);

			bb.putInt(id).putInt(treeLabel);

			bb.put((r == null) ? new byte[nonceLen] : r);

			return bb.array();
		}

		public String toString() {return Arrays.toString(toByteArray());}

		private void enc() {
			r = TreeBasedUtils.genPRBits(rnd, nonceLen);
			mask();
		}

		public void dec() {
			mask();
			r = null;
		}

		private void mask() {
			byte[] mask = new byte[extDataSize];
			try {
				MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
				int hashLength = 20;
				int i = 0;
				for (; (i+1)*hashLength < extDataSize; i++) {
					sha1.update(clientKey);
					sha1.update(r);
					sha1.update(ByteBuffer.allocate(4).putInt(i));
					System.arraycopy(sha1.digest(), 0, mask, i*hashLength, hashLength);
				}
				sha1.update(clientKey);
				sha1.update(r);
				sha1.update(ByteBuffer.allocate(4).putInt(i));
				System.arraycopy(sha1.digest(), 0, mask, i*hashLength, extDataSize-i*hashLength);

				BitSet dataMask = BitSet.valueOf(Arrays.copyOfRange(mask, 0, dataSize));
				data.xor(dataMask);
				id ^= ByteBuffer.wrap(mask, dataSize, 4).getInt();
				treeLabel ^= ByteBuffer.wrap(mask, dataSize+4, 4).getInt();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public class Bucket {
		public Block[] blocks = new Block[Z];

		public Bucket(Block b) {
			assert (b != null) : "No null block pointers allowed.";
			blocks[0] = b;
			for (int i = 1; i < Z; i++)
				blocks[i] = new Block();
		}

		Bucket(byte[] array) {
			ByteBuffer bb = ByteBuffer.wrap(array);
			byte[] temp = new byte[extDataSize+nonceLen];
			for (int i = 0; i < Z; i++) {
				bb.get(temp);
				blocks[i] = new Block(temp);
			}
		}

		public byte[] toByteArray() {
			ByteBuffer bb = ByteBuffer.allocate(Z * (extDataSize+nonceLen));
			for (Block blk : blocks)
				bb.put(blk.toByteArray());
			return bb.array();
		}

		public void encryptBlocks() {
			for (Block blk : blocks)
				blk.enc();
		}
	}

	public class Stash
	{
		public LocalStorage ls = null;
		public List<Block> blocks = null;
		public int size = 0; // in blocks

		public Stash(int size, int recLevel, boolean useLS)
		{
			this.ls = null;
			this.blocks = null;
			this.size = size;

			if(useLS == true)
			{
				ls = new LocalStorage("/tmp/Local" + recLevel, true);
				ls.connect();

				for (int i = 0; i < size; i++)
				{
					String objectKey = recLevel + "#" + (i);
					DataItem di = new SimpleDataItem(new Block().toByteArray());
					UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
					ScheduledOperation sop = ls.uploadObject(upload);
					sop.waitUntilReady();
				}
			}
			else
			{
				// use list of blocks (in memory)
				blocks = new ArrayList<Block>();
			}
		}

		public void save(ObjectOutputStream os) throws IOException
		{
			os.writeInt(size);
			os.writeInt(recLevel);

			boolean useLS = (ls != null);
			os.writeBoolean(useLS);

			if(useLS == true)
			{
				for (int i = 0; i < size; i++)
				{
					String objectKey = recLevel + "#" + (i);
					DownloadOperation download = new DownloadOperation(Request.initReqId, objectKey);
					ScheduledOperation sop = ls.downloadObject(download);
					sop.waitUntilReady();

					byte[] data = sop.getDataItem().getData();

						/*{ // tmp debug
							Block blk = new Block(data, true);
							if(blk.isDummy() == false)
							{ log.append("[POB (saveStash)] Saving block with id " + blk.id, Log.TRACE); }
						}*/

					os.writeInt(data.length);
					os.write(data);
				}
			}
			else
			{
				os.writeInt(blocks.size());
				for(int i=0; i<blocks.size(); i++)
				{
					Block blk = blocks.get(i);
					byte[] data = blk.toByteArray();


						/*{ // tmp debug
							if(blk.isDummy() == false)
							{ log.append("[POB (saveStash)] Saving block with id " + blk.id, Log.TRACE); }
						}*/

					os.writeInt(data.length);
					os.write(data);
				}
			}
		}

		public Stash(ObjectInputStream is) throws IOException
		{
			this.ls = null;
			this.blocks = null;
			size = is.readInt();

			int tmpRecLevel = is.readInt();

			boolean useLS = is.readBoolean();

			if(useLS == true)
			{
				ls = new LocalStorage("/tmp/Local" + tmpRecLevel, true);
				ls.connect();

				for (int i = 0; i < size; i++)
				{
					int byteSize = is.readInt();
					byte[] data = new byte[byteSize];
					is.readFully(data);

						/*{ // tmp debug
							Block blk = new Block(data, true);
							if(blk.isDummy() == false)
							{ log.append("[POB (loadStash)] Loaded block with id " + blk.id, Log.TRACE); }
						}*/

					String objectKey = recLevel + "#" + (i);
					DataItem di = new SimpleDataItem(data);
					UploadOperation upload = new UploadOperation(Request.initReqId, objectKey, di);
					ScheduledOperation sop = ls.uploadObject(upload);
					sop.waitUntilReady();
				}
			}
			else
			{
				blocks = new ArrayList<Block>();

				int s = is.readInt();
				for(int i=0; i<s; i++)
				{
					int byteSize = is.readInt();
					byte[] data = new byte[byteSize];
					is.readFully(data);

					Block blk = new Block(data, true);

						/*{ // tmp debug
							if(blk.isDummy() == false)
							{ log.append("[POB (loadStash)] Loaded block with id " + blk.id, Log.TRACE); }
						}*/

					blocks.add(blk);
				}
			}
		}
	}

	public void save(ObjectOutputStream os) throws IOException
	{
		os.writeInt(N);
		os.writeInt(D);

		os.writeLong(treeSize);

		stash.save(os);
	}

	public Tree(ExternalStorageInterface si, ObjectInputStream is)  throws IOException
	{
		storedTree = si;
		N = is.readInt();
		D = is.readInt();

		treeSize = is.readLong();

		stash = new Stash(is);
	}
}