package eoram.cloudexp.schemes.primitives.ringoram;

public class Configs
{
	//the max real block count in the bucket
	public static int REAL_BLOCK_COUNT = 33; //Chosen according to Table 5 in the RingORAM paper

	//the min dummy block count in the bucket
	public static int DUMMY_BLOCK_COUNT = 63; //Calculated according to Table 4 in the RingORAM paper

	//shuffle rate
	public static int SHUFFLE_RATE = 48; //Chosen according to Table 5 in the RingORAM paper

	//total block count in bucket
	public static int Z = REAL_BLOCK_COUNT + DUMMY_BLOCK_COUNT;
	
	//read_counter, meta_buf, valid_bits
	public static int METADATA_BYTES_LEN = 4+4*(Configs.REAL_BLOCK_COUNT + Configs.Z)+Configs.Z;

	//Stash size, extrapolated from Table 3 in the RingORAM paper
	public static int STASH_SIZE = 115;

	/*
	* Variables below are calculated on startup
	*/

	//block data length
	public static int BLOCK_DATA_LEN = -1;

	//total bucket count in the tree, must be full binary tree
	public static int BUCKET_COUNT = -1;

	//total block count in the tree
	public static int BLOCK_COUNT = -1;

	//tree height
	public static int HEIGHT = -1;

	//total leaf count in the tree
	public static int LEAF_COUNT = -1;

	//leaf start index in tree node(root is 0)
	public static int LEAF_START = -1;

	//request operation: read or write
	public enum OPERATION{ORAM_ACCESS_READ,ORAM_ACCESS_WRITE};
}
