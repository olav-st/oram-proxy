package eoram.cloudexp.schemes.primitives;

import eoram.cloudexp.data.BlockDataItem;

/**
 * Interface to create dummy blocks.
 */
public interface DummyFactory 
{
	public boolean isDummy(String key);
	public BlockDataItem create();
}
