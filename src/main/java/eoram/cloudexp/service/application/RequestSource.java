package eoram.cloudexp.service.application;

import eoram.cloudexp.service.Request;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a "source" of requests.
 *
 */
public abstract class RequestSource 
{
	protected ConcurrentHashMap<Long, Request> requests = new ConcurrentHashMap<>();
	protected long nextReqId = -1;
	
	protected RequestSource() {}
	
	public void seek(long reqId) { nextReqId = reqId; }
	/*public void seek(long reqId)
	{
		nextReqId = requests.ceilingKey(reqId);
	}*/
	
	public boolean hasNext() 
	{ 
		return requests.containsKey(nextReqId);
	}
	
	public void rewind() { nextReqId = requests.keySet().iterator().next(); }
	
	public Request next()
	{
		if(nextReqId == -1)	{ rewind(); }		
		return requests.get(nextReqId++);
	}
	
	public void dumpToFile(File outputFile)
	{
		try
		{
			FileWriter fw = new FileWriter(outputFile);
			BufferedWriter bw = new BufferedWriter(fw);
			
			for(long reqId : requests.keySet())
			{
				Request req = requests.get(reqId);
				bw.write(req.toString()); bw.newLine();
			}
			
			bw.flush();
			bw.close();
		}
		catch(Exception e) { throw new RuntimeException(e); }
	}

	public void close() {}

	public int getSize() { return requests.size(); }

	public long getNextRequestId() { return nextReqId; }
}
