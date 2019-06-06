package eoram.cloudexp.implementation;

import eoram.cloudexp.artifacts.SessionState;
import eoram.cloudexp.artifacts.SystemParameters;
import eoram.cloudexp.data.EmptyDataItem;
import eoram.cloudexp.data.SimpleDataItem;
import eoram.cloudexp.data.WrappedListDataItem;
import eoram.cloudexp.interfaces.InternalStorageInterface;
import eoram.cloudexp.service.*;
import eoram.cloudexp.utils.Errors;
import eoram.cloudexp.utils.MiscUtils;
import eoram.cloudexp.utils.SwiftUtils;
import org.javaswift.joss.headers.object.range.MidPartRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implements a (synchronous) storage interface for Swift.
 *
 *  @see SwiftAsyncStorage
 */
public class SwiftStorage implements InternalStorageInterface
{
	private SystemParameters sysParams = SystemParameters.getInstance();
	private SwiftUtils utils = SwiftUtils.getInstance();

	private Account account = null;
	private Container container = null;

	private String containerName = null;

	private boolean resetContainer = false;

	public SwiftStorage(boolean shouldReset) { resetContainer = shouldReset; }

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

	@Override
	public ScheduledOperation downloadObject(DownloadOperation op)
	{
		String key = op.getKey();
		DownloadInstructions instructions = new DownloadInstructions();
		if(op.getNumBytes() != -1)
		{
			MidPartRange range = new MidPartRange(op.getOffset(), op.getOffset() + op.getNumBytes());
			instructions.setRange(range);
		}

		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;

		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);

			StoredObject o = container.getObject(key);
			InputStream is = o.downloadObjectAsInputStream(instructions);

			SimpleDataItem sdi = new SimpleDataItem(MiscUtils.getInstance().ByteArrayFromInputStream(is));
			sop.onSuccess(sdi);
			try { is.close(); } catch (IOException e) { Errors.error(e); }

			return sop;
		}

		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation uploadObject(UploadOperation op)
	{
		String key = op.getKey();
		byte[] data = op.getDataItem().getData();

		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;

		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);

			StoredObject o = container.getObject(key);
			o.uploadObject(new ByteArrayInputStream(data));

			sop.onSuccess(new EmptyDataItem());
			return sop;
		}

		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation deleteObject(DeleteOperation op)
	{
		String key = op.getKey();

		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;

		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);

			StoredObject o = container.getObject(key);
			o.delete();

			sop.onSuccess(new EmptyDataItem());
			return sop;
		}

		sop.onFailure();
		return sop;
	}

	@Override
	public ScheduledOperation copyObject(CopyOperation op)
	{
		String srcKey = op.getSourceKey();
		String destKey = op.getDestinationKey();

		int storageOpMaxAttempts = sysParams.storageOpMaxAttempts;

		ScheduledOperation sop = new ScheduledOperation(op);
		for(int attempt = 0; attempt < storageOpMaxAttempts; attempt++)
		{
			boolean lastAttempt = (attempt == storageOpMaxAttempts - 1);

			StoredObject srcObject = container.getObject(srcKey);
			StoredObject destObject = container.getObject(destKey);
			srcObject.copyObject(container, destObject);
		}

		sop.onFailure();
		return sop;
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
	public void disconnect() {}

	@Override
	public long totalByteSize()
	{
		return container.getBytesUsed();
	}

	@Override
	public void cloneTo(String to)
	{
		utils.cloneContainer(account, container, to);
	}
}
