package eoram.cloudexp.service;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.pollables.Completable;
import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.utils.Errors;

/**
 * Represents a scheduled operation.
 * <p><p>
 * A scheduled operation represents an operation that has been scheduled but may be pending (i.e., not completed yet).
 */
public class ScheduledOperation extends Pollable implements Completable
{
	protected boolean completed = false;
	protected Operation operation = null;
	
	protected boolean success = false;
	
	protected DataItem dataItem = null;
	
	public ScheduledOperation(Operation op) { operation = op; completed = false; dataItem = null; success = false; }
	
	public synchronized boolean isReady() { return completed; }

	public synchronized void onSuccess(DataItem d) 
	{
		completed = true;
		success = true;
		dataItem = d;
	}
	
	public synchronized void onFailure()
	{
		completed = true;
		success = false;
		dataItem = null;
		
		Errors.error("CODING FAIL!");
	}

	public synchronized DataItem getDataItem()
	{
		waitUntilReady();
		return dataItem;
	}

	public Operation getOperation() { return operation; }

	public synchronized boolean wasSuccessful() // will block until ready
	{
		if(completed == false) { waitUntilReady(); }
		return success;
	}

	public void save(ObjectOutputStream os) throws Exception
	{
		os.writeBoolean(completed);
		
		// save op (workaround for ObliviStore) // TODO: do things correctly later
		//os.writeObject(operation);
		boolean operationNotNull = operation != null;
		os.writeBoolean(operationNotNull);
		
		if(operationNotNull == true)
		{
			os.writeLong(operation.reqId);
			os.writeLong(operation.opId);
			os.writeObject(operation.key);
		}
		
		os.writeBoolean(success);
		
		Errors.verify(dataItem != null);
		os.writeObject(dataItem.toString());
	}
	
	public ScheduledOperation(ObjectInputStream is) throws Exception
	{
		completed = is.readBoolean();
		
		// load op (workaround for ObliviStore) // TODO: do things correctly later
		//operation = (Operation) is.readObject();
		
		operation = null;
		boolean operationNotNull = is.readBoolean();
		
		if(operationNotNull == true)
		{
			long reqId = is.readLong();
			long opId = is.readLong();
			String key = (String) is.readObject();		
			operation = new DownloadOperation(reqId, opId, key);
		}
		
		success = is.readBoolean();
		
		String fromString = (String)is.readObject();
		dataItem = new SimpleDataItem(fromString);
	}
}
