package eoram.cloudexp.threading;

/**
 * Represents a jobs queue with a single thread.
 */
public class SingleThreadJobsQueue extends ThreadedJobsQueue 
{
	public SingleThreadJobsQueue() { super(1); }
}
