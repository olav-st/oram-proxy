package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.artifacts.SystemParameters;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.TempFileDataItem;
import eoram.cloudexp.data.WrappedListDataItem;
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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements an asynchronous storage interface for Swift.
 *
 * <p>
 * @see SwiftStorage
 */
public class SwiftAsyncStorage implements InternalStorageInterface
{
	private Log log = Log.getInstance();
	private SystemParameters sysParams = SystemParameters.getInstance();

	private SwiftUtils utils = SwiftUtils.getInstance();

	private ExecutorService executor = Executors.newFixedThreadPool(128); // Executors.newCachedThreadPool();

	private Account account = null;
	private Container container = null;

	private String containerName = null;

	private boolean resetContainer = false;

	public SwiftAsyncStorage(boolean shouldReset) { resetContainer = shouldReset; }

	private AtomicInteger pendingOpsCount = new AtomicInteger(0);

	public class SwiftStorageCallback
	{
		private ScheduledOperation sop = null;
		private int attempt = 0;
		private DataItem dataItem = null;

		public SwiftStorageCallback(ScheduledOperation op)
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

					String msg = "[AsyncSwift] Operation(" + opId + ", " + opType + ", " + opKey + ") from reqId: " + reqId;
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
		containerName = SessionState.getInstance().storageKey.toLowerCase();
		account = utils.initialize(sysParams.credentials, containerName, resetContainer);
		container = account.getContainer(containerName);
		if(resetContainer && container.exists())
		{
			container.delete();
		}
		container.create();
	}

	private void _downloadObject(ScheduledOperation sop, final SwiftStorageCallback listener)
	{
		assert(listener != null);
		DownloadOperation op = (DownloadOperation)sop.getOperation();

		// -d-
		//{ log.append("[AsyncSwift] Downloading key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }

		String key = op.getKey();
		DownloadInstructions instructions = new DownloadInstructions();
		if(op.getNumBytes() != -1)
		{
			MidPartRange range = new MidPartRange(op.getOffset(), op.getOffset() + op.getNumBytes());
			instructions.setRange(range);
		}

		TempFileDataItem d = new TempFileDataItem();
		listener.setDataItem(d);

		StoredObject object = container.getObject(key);
		pendingOpsCount.incrementAndGet(); // increment the counter

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { object.downloadObject(d.getFile(), instructions); listener.onSuccess();	}
				catch (Exception e) { listener.onFailure(e); }
			}
		});
	}

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_downloadObject(sop, new SwiftStorageCallback(sop));

		return sop;
	}

	private void _uploadObject(ScheduledOperation sop, final SwiftStorageCallback listener)
	{
		assert(listener != null);
		UploadOperation op = (UploadOperation)sop.getOperation();

		// -d-
		//{ log.append("[AsyncSwift] Uploading key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }	

		byte[] data = op.getDataItem().getData();

		listener.setDataItem(new EmptyDataItem()); // empty data item for uploads

		StoredObject object = container.getObject(op.getKey());
		pendingOpsCount.incrementAndGet(); // increment the counter

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { object.uploadObject(new ByteArrayInputStream(data)); listener.onSuccess();	}
				catch (Exception e) { listener.onFailure(e); }
			}
		});

	}

	@Override
	public ScheduledOperation uploadObject(UploadOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_uploadObject(sop, new SwiftStorageCallback(sop));

		return sop;
	}

	private void _deleteObject(final ScheduledOperation sop, final SwiftStorageCallback listener)
	{
		assert(listener != null);
		final DeleteOperation op = (DeleteOperation)sop.getOperation();

		// -d-
		//{ log.append("[AsyncSwift] Deleting key " + op.getKey() + ", for req " +  op.getRequestId() + "(op: " + op.getOperationId() + ")", Log.TRACE); }	


		listener.setDataItem(new EmptyDataItem()); // empty data items for copies
		StoredObject obj = container.getObject(op.getKey());

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { obj.delete(); listener.onSuccess(); }
				catch (Exception e) { listener.onFailure(e); }
			}
		});
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_deleteObject(sop, new SwiftStorageCallback(sop));

		return sop;
	}

	@Override
	public ScheduledOperation copyObject(CopyOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);

		_copyObject(sop, new SwiftStorageCallback(sop));

		return sop;
	}

	private void _copyObject(final ScheduledOperation sop, final SwiftStorageCallback listener)
	{
		assert(listener != null);
		final CopyOperation op = (CopyOperation)sop.getOperation();

		listener.setDataItem(new EmptyDataItem()); // empty data items for copies

		String srcKey = op.getSourceKey();
		String destKey = op.getDestinationKey();
		StoredObject srcObject = container.getObject(srcKey);
		StoredObject destObject = container.getObject(destKey);

		executor.submit(new Runnable()
		{
			@Override
			public void run()
			{
				try { srcObject.copyObject(container, destObject); listener.onSuccess(); }
				catch (Exception e) { listener.onFailure(e); }
			}
		});
	}

	@Override
	public ScheduledOperation listObjects(ListOperation op)
	{
		ScheduledOperation sop = new ScheduledOperation(op);
		List<String> ret = new ArrayList<String>();

		Collection<StoredObject> objects = container.list();
		for(StoredObject o : objects) { ret.add(o.getName()); }

		sop.onSuccess(new WrappedListDataItem(ret));
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
		while(pendingOpsCount.get() > 0)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { e.printStackTrace(); }
		}
		return container.getBytesUsed();
	}

	@Override
	public void cloneTo(String to)
	{
		while(pendingOpsCount.get() > 0)
		{
			try { Thread.sleep(5); } catch (InterruptedException e) { e.printStackTrace(); }
		}
		utils.cloneContainer(account, container, to);
	}
}
