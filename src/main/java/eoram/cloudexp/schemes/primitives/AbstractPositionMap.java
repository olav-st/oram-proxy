package eoram.cloudexp.schemes.primitives;

import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;


/**
 * Represents an abstract position map
 * @param <PositionType>
 */
public abstract class AbstractPositionMap<PositionType>
{
	public AbstractPositionMap() { }
	
	protected Map<String, PositionType> map = new HashMap<>();

	public void clear() { map.clear(); }
	
	public PositionType getPos(String key) 
	{
		PositionType pos = map.get(key); 
		// -d-
		//{ Log.getInstance().append("[AP] " + key + " is at " + pos, Log.TRACE); }
		return pos;
	}
	
	public PositionType setPos(String key, PositionType pos) 
	{
		PositionType oldPos = map.put(key, pos);
		// -d-
		//{ Log.getInstance().append("[AP] " + key + ": " + oldPos + " -> " + pos, Log.TRACE); }
		return oldPos; 
	}

	public void removeKey(String key) { map.remove(key); }
	public int getSize() { return map.size(); }
	
	public abstract void save(ObjectOutputStream os) throws Exception;
	
}
