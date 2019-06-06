package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.artifacts.SystemParameters;
import eoram.cloudexp.data.*;
import eoram.cloudexp.interfaces.InternalStorageInterface;
import eoram.cloudexp.service.*;
import eoram.cloudexp.utils.Errors;
import eoram.cloudexp.utils.SwiftUtils;
import org.javaswift.joss.headers.object.range.MidPartRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements an asynchronous storage interface for InMemoryStorage.
 *
 * <p>
 * @see SwiftStorage
 */
public class AsyncInMemoryStorage implements InternalStorageInterface
{
	private Log log = Log.getInstance();
	private SystemParameters sysParams = SystemParameters.getInstance();

	private ExecutorService executor = Executors.newFixedThreadPool(128); // Executors.newCachedThreadPool();

	private HashMap<String, byte[]> map = new HashMap();

	public AsyncInMemoryStorage(boolean reset) {}

	private AtomicInteger pendingOpsCount = new AtomicInteger(0);

	public class InMemoryStorageCallback
	{
		private ScheduledOperation sop = null;
		private int attempt = 0;
		private DataItem dataItem = null;

		public InMemoryStorageCallback(ScheduledOperation op)
		{
			sop = op;
			attempt = 0;
		}

		public void setDataItem(DataItem d) { dataItem = d; }

		public void onSuccess()
		{
			pendingOpsCount.decrementAndGet(); // decrement
			sop.onSuccess(dataItem);

			attempt++;
		}

		public void onFailure(Exception e)
		{
			pendingOpsCount.decrementAndGet(); // decrement

			attempt++;
			boolean isDelete = sop.getOperation().getType() == Operation.OperationType.DELETE;
			if(attempt >= sysParams.storageOpMaxAttempts)
			{
				if(isDelete == true) { pendingOpsCount.incrementAndGet(); onSuccess(); } // fix for deletes of invalid keys -> make it a success
				else
				{
					sop.onFailure();
					Errors.error(e);
				}
			}
			else  // retry
			{
				Errors.warn(e);
				{
					long reqId = sop.getOperation().getRequestId();
					long opId = sop.getOperation().getOperationId();
					Operation.OperationType opType = sop.getOperation().getType();
					String opKey = sop.getOperation().getKey();

					String msg = "[AsyncInMemory] Operation(" + opId + ", " + opType + ", " + opKey + ") from reqId: " + reqId;
					msg += " has failed (attempt: " + attempt + ") -> Retrying...";
					Errors.warn(msg);
				}

				Operation.OperationType type = sop.getOperation().getType();
				switch(type)
				{
					case DOWNLOAD: _downloadObject(sop, this); break;
					case UPLOAD: _uploadObject(sop, this); break;
					case DELETE: _deleteObject(sop, this); break;
					case COPY: _copyObject(sop, this); break;
					default:
						assert(true == false); Errors.error("Coding FAIL!"); break;
				}


			}
		}
	}

	@Override
	public void connect()
	{
	}

	private void _downloadObject(ScheduledOperation sop, final InMemoryStorageCallback listener)
	{
		assert(listener != null);
		DownloadOperation op = (DownloadOperation)sop.getOperation();

		// -d-
		//{ log.append("[AsyncInMemory] Downloading key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }

		String key = op.getKey();
		DownloadInstructions instructions = new DownloadInstructions();
		if(op.getNumBytes() != -1)
		{
			MidPartRange range = new MidPartRange(op.getOffset(), op.getOffset() + op.getNumBytes());
			instructions.setRange(range);
		}

		SimpleDataItem d = new SimpleDataItem(new byte[0]);
		listener.setDataItem(d);

		pendingOpsCount.incrementAndGet(); // increment the counter

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { d.setData(map.get(key)); listener.onSuccess();	}
				catch (Exception e) { listener.onFailure(e); }
			}
		});
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_downloadObject(sop, new InMemoryStorageCallback(sop));

		return sop;
	}

	private void _uploadObject(ScheduledOperation sop, final InMemoryStorageCallback listener)
	{
		assert(listener != null);
		UploadOperation op = (UploadOperation)sop.getOperation();

		// -d-
		//{ log.append("[AsyncInMemory] Uploading key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }	

		byte[] data = op.getDataItem().getData();

		listener.setDataItem(new EmptyDataItem()); // empty data item for uploads

		pendingOpsCount.incrementAndGet(); // increment the counter

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { map.put(op.getKey(), data); listener.onSuccess();	}
				catch (Exception e) { listener.onFailure(e); }
			}
		});

	}

	@Override
	public ScheduledOperation uploadObject(UploadOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_uploadObject(sop, new InMemoryStorageCallback(sop));

		return sop;
	}

	private void _deleteObject(final ScheduledOperation sop, final InMemoryStorageCallback listener)
	{
		assert(listener != null);
		final DeleteOperation op = (DeleteOperation)sop.getOperation();

		// -d-
		//{ log.append("[AsyncInMemory] Deleting key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }	


		listener.setDataItem(new EmptyDataItem()); // empty data items for copies

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { map.remove(op.getKey()); listener.onSuccess(); }
				catch (Exception e) { listener.onFailure(e); }
			}
		});
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_deleteObject(sop, new InMemoryStorageCallback(sop));

		return sop;
	}

	@Override
	public ScheduledOperation copyObject(CopyOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_copyObject(sop, new InMemoryStorageCallback(sop));

		return sop;
	}

	private void _copyObject(final ScheduledOperation sop, final InMemoryStorageCallback listener)
	{
		assert(listener != null);
		final CopyOperation op = (CopyOperation)sop.getOperation();

		listener.setDataItem(new EmptyDataItem()); // empty data items for copies

		String srcKey = op.getSourceKey();
		String destKey = op.getDestinationKey();
		byte[] srcData = map.get(srcKey);

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try {
					if(srcData != null)
					{
						map.put(destKey, srcData);
						sop.onSuccess(new EmptyDataItem());
					}
					else
					{
						throw new IllegalStateException("srcData is null");
					}
					listener.onSuccess();
				}
				catch (Exception e) { listener.onFailure(e); }
			}
		});
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
	public void disconnect()
	{
		while(pendingOpsCount.get() > 0)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { e.printStackTrace(); }
		}

		executor.shutdownNow();
		try { executor.awaitTermination(0, TimeUnit.SECONDS); }
		catch (InterruptedException e) { e.printStackTrace(); }
	}

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
	}
}
