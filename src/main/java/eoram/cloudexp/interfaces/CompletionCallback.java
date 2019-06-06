package eoram.cloudexp.interfaces;

import eoram.cloudexp.service.ScheduledRequest;

/**
 * Defines (the public interface of) a completion callback.
 */
public interface CompletionCallback 
{
	public void onSuccess(ScheduledRequest scheduled);
	public void onFailure(ScheduledRequest scheduled);
}
