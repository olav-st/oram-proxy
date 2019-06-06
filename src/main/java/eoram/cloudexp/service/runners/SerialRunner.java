package eoram.cloudexp.service.runners;

import eoram.cloudexp.interfaces.CompletionCallback;
import eoram.cloudexp.interfaces.ExternalClientInterface;
import eoram.cloudexp.interfaces.ExternalStorageInterface;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledRequest;

/**
 * Runs requests serially (i.e., one at a time).
 */
public class SerialRunner extends AbstractRunner 
{
	public SerialRunner(ExternalClientInterface c, ExternalStorageInterface s) { super(c, s); }

	@Override
	public boolean onScheduled(ScheduledRequest sreq) 
	{
		sreq.waitUntilReady();		// force serialization (i.e., no parallelism)
		return sreq.wasSuccessful(); 
	}

	@Override
	public CompletionCallback onNew(Request req) { return null; } 
}
