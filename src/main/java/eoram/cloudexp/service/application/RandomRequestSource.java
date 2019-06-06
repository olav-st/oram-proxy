package eoram.cloudexp.service.application;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.InflatableDataItem;
import eoram.cloudexp.service.*;

/**
 * Implements a request source of random requests.
 * This class can be used to generate random requests.
 */
public class RandomRequestSource extends RequestSource
{	
	public RandomRequestSource(int initLength, int length)
	{
		this(initLength, length, 0.25, 0.75);
	}
	
	public RandomRequestSource(int initLength, int length, double putProb, double newKeyProb)
	{
		super();
		
		List<String> putList = randomSequence(initLength, 1.0, 1.0, new ArrayList<String>());
		if(length > initLength) { randomSequence(length - initLength, putProb, newKeyProb, putList); }
		
		rewind();
	}
	
	private ClientParameters clientParams = ClientParameters.getInstance();
	
	private List<String> randomSequence(int length, double putProb, double newKeyProb, List<String> putList)
	{	
		Random rng = new SecureRandom();
		
		Set<String> putSet = new HashSet<String>();
		int nextPutKey = 0; boolean maxKeyReached = false;
		
		for(int i=0; i < length; i++)
		{		
			final double p1 = putProb;
			final double p2 = newKeyProb;
			
			Request req = null;
			
			if(rng.nextDouble() <= p1) // put
			{
				String key = null;
				
				//if(length < 2000){
				if(i == 0 || rng.nextDouble() <= p2) 
				{
					key = "" + nextPutKey;
					
					if(maxKeyReached == true) { nextPutKey = rng.nextInt((int)clientParams.maxBlocks);  }
					else { nextPutKey++; }
					
					if(nextPutKey >= clientParams.maxBlocks -1)	{ maxKeyReached = true; }
				}
				else
				{ 
					// take one of the put keys
					key = putList.get(rng.nextInt(putList.size()));
				}
				//} else { key = putList.get(rng.nextInt(4)); }

				if(putSet.contains(key) == false)
				{
					putSet.add(key);
					putList.add(key);
				}
				
				DataItem di = new InflatableDataItem(rng.nextInt(), clientParams.contentByteSize);
				req = new PutRequest(key, di);
			}
			else // get
			{
				// take one of the put keys
				String key = putList.get(rng.nextInt(putList.size()));
				//String key = putList.get(rng.nextInt(4));
				
				req = new GetRequest(key);
			}
			
			requests.put(req.getId(), req);
		}
		return putList;
	}
}
