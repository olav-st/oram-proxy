package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.WrappedListDataItem;
import eoram.cloudexp.interfaces.InternalStorageInterface;
import eoram.cloudexp.service.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Implements a (synchronous) storage interface for a HashMap in memory.
 * @see AsyncLocalStorage
 */
public class InMemoryStorage implements InternalStorageInterface
{
	private Log log = Log.getInstance();
	private HashMap<String, byte[]> map = new HashMap();

	public InMemoryStorage(boolean reset)
	{
	}

	@Override
	public void connect()
	{
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op)
	{
		String key = op.getKey();

		ScheduledOperation sop = new ScheduledOperation(op);
		{
			byte[] d = map.get(key);

			if(d != null)
			{
				if(op.getNumBytes() != -1)
				{
					d = Arrays.copyOfRange(d, op.getOffset(), op.getOffset() + op.getNumBytes());
				}
				sop.onSuccess(new SimpleDataItem(d));
			}else
			{
				sop.onFailure();
			}

		}

		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadOperation op)
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();

		ScheduledOperation sop = new ScheduledOperation(op);
		{
			map.put(key, data);
			sop.onSuccess(new EmptyDataItem());
		}
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op)
	{
		String key = op.getKey();

		ScheduledOperation sop = new ScheduledOperation(op);
		{
			if(map.containsKey(key))
			{
				map.remove(key);
				sop.onSuccess(new EmptyDataItem());
			}
			else
			{
				sop.onFailure();
			}
		}
		return sop;
	}

	@Override
	public ScheduledOperation copyObject(CopyOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		{
			String srcKey = op.getSourceKey();
			String dstKey = op.getDestinationKey();

			byte[] srcData = map.get(srcKey);
			if(srcData != null)
			{
				map.put(dstKey, srcData);
				sop.onSuccess(new EmptyDataItem());
			}
			else
			{
				sop.onFailure();
			}
		}
		return sop;
	}

	@Override
	public ScheduledOperation listObjects(ListOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		List<String> l = new ArrayList<String>(map.keySet());
		sop.onSuccess(new WrappedListDataItem(l));

		return sop;
	}

	@Override
	public void disconnect() { ; }

	@Override
	public long totalByteSize()
	{
		int sum = 0;
		for(byte[] b : map.values())
		{
			sum += b.length;
		}
		return sum;
	}

	@Override
	public void cloneTo(String to)
	{
		//TODO
	}
}
