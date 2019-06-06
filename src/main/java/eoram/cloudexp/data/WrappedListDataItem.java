package eoram.cloudexp.data;

import java.util.ArrayList;
import java.util.List;

import eoram.cloudexp.utils.Errors;

/**
 * Represents a data item containing an encoding of a String list.
 * <p><p>
 * The methods {@code getData} and {@code toString} are not supported!
 */
public class WrappedListDataItem extends DataItem 
{

	protected List<String> list = new ArrayList<String>();
	
	public WrappedListDataItem(List<String> ret) { list = ret; }

	@Override
	public byte[] getData() 
	{ 
		Errors.verify(false, "getData() is unsupported for WrappedListDataItem");
		return null;
	}

	public String toString()
	{
		Errors.verify(false, "getData() is unsupported for WrappedListDataItem");
		return null;
	}
	
	public synchronized List<String> getList() { return list; }
}
