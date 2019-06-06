package eoram.cloudexp.service.application.http.swiftapi.utils;

import eoram.cloudexp.artifacts.ClientParameters;
import eoram.cloudexp.service.application.http.swiftapi.resources.SwiftObject;
import eoram.cloudexp.utils.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class SwiftOramMap
{
	private AtomicLong nextOramKey = new AtomicLong(0);
	private Map<String, SwiftObject> oramKeyToSwiftObjectMap = new ConcurrentHashMap<>();
	private Map<SwiftObject, List<String>> swiftObjectToOramKeyMap = new ConcurrentHashMap<>();

	ClientParameters clientParams = ClientParameters.getInstance();

	public void addOramKey(String key, SwiftObject object)
	{
		oramKeyToSwiftObjectMap.put(key, object);
		List<String> oramKeys = swiftObjectToOramKeyMap.get(object);
		if(oramKeys == null)
		{
			oramKeys = new CopyOnWriteArrayList<>();
		}
		oramKeys.add(key);
		swiftObjectToOramKeyMap.put(object, oramKeys);
	}

	public void removeOramKey(String key)
	{
		SwiftObject object = oramKeyToSwiftObjectMap.get(key);
		oramKeyToSwiftObjectMap.remove(key);
		swiftObjectToOramKeyMap.remove(object);
	}

	public void removeSwiftObject(SwiftObject object)
	{
		List<String> keys = getOramKeys(object);
		if(keys != null)
		{
			for(String key : keys)
			{
				oramKeyToSwiftObjectMap.remove(key);
			}
		}
		swiftObjectToOramKeyMap.remove(object);
	}

	public List<String> getOramKeys(SwiftObject object)
	{
		if(swiftObjectToOramKeyMap.containsKey(object))
		{
			return swiftObjectToOramKeyMap.get(object);
		}
		return new ArrayList<>();
	}

	public SwiftObject getObject(String key)
	{
		return oramKeyToSwiftObjectMap.get(key);
	}

	public String getNextOramKey()
	{
		if(oramKeyToSwiftObjectMap.size() >= clientParams.maxBlocks)
		{
			Errors.error("No free keys");
		}

		long key = 0;
		while(oramKeyToSwiftObjectMap.containsKey(""+ key))
		{
			key = nextOramKey.getAndIncrement() % clientParams.maxBlocks;
		}
		return "" + key;
	}
}
