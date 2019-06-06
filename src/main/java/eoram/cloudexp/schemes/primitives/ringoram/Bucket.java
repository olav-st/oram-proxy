package eoram.cloudexp.schemes.primitives.ringoram;

import java.io.Serializable;

public class Bucket implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	int id;//bucket id in the tree, root is 0
	byte[] bucket_data;//all block data in the bucket
	
	public Bucket(){
		this.id = 0;
		this.bucket_data = new byte[Configs.BLOCK_DATA_LEN * Configs.Z];
	}
	
	public Bucket(int id, byte[] bucket_data){
		this.id = id;
		this.bucket_data = bucket_data;
	}
	
	//get block data from the bucket data
	public byte[] getBlock(int offset){
		int startIndex = offset * Configs.BLOCK_DATA_LEN;
		byte[] returndata = new byte[Configs.BLOCK_DATA_LEN];
		for(int i = 0; i< Configs.BLOCK_DATA_LEN; i++){
			returndata[i] = bucket_data[startIndex+i];
		}
		return returndata;
	}

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public byte[] getBucket_data() {
		return bucket_data;
	}
	public void setBucket_data(byte[] bucket_data) {
		this.bucket_data = bucket_data;
	}

}
