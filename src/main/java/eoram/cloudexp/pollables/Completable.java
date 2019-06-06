package eoram.cloudexp.pollables;

import eoram.cloudexp.data.DataItem;

/**
 * Defines the interface of something that can be completed.
 */
public interface Completable 
{
	public void onSuccess(DataItem d);
	public void onFailure();
}
