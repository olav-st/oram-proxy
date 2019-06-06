package eoram.cloudexp.service.application.http.swiftapi;

import eoram.cloudexp.artifacts.Log;
import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.data.VariableLengthDataItem;
import eoram.cloudexp.service.GetRequest;
import eoram.cloudexp.service.PutRequest;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.application.http.HttpRequestSourceServlet;
import eoram.cloudexp.service.application.http.swiftapi.resources.*;
import eoram.cloudexp.service.application.http.swiftapi.utils.HTTPUtils;
import eoram.cloudexp.service.application.http.swiftapi.utils.SerializationUtils;
import eoram.cloudexp.service.application.http.swiftapi.utils.SwiftOramMap;
import eoram.cloudexp.utils.Errors;
import eoram.cloudexp.utils.MiscUtils;
import org.eclipse.jetty.io.EofException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for the OpenStack Swift HTTP API
 *
 */
public class SwiftResourceServlet extends HttpRequestSourceServlet
{
	private Log log = Log.getInstance();

	private Map<Long, AsyncContext> asyncContexts = new ConcurrentHashMap<>();
	private Map<AsyncContext, Map<Request, Integer>> pendingOramRequests = new ConcurrentHashMap<>();
	private Map<AsyncContext, Map<Integer, Request>> completedOramRequests = new ConcurrentHashMap<>();
	private Map<AsyncContext, Map<Integer, DataItem>> completedOramDataItems = new ConcurrentHashMap<>();
	private Map<AsyncContext, Integer> nextOramRequest = new ConcurrentHashMap<>();
	private SwiftOramMap swiftOramMap = new SwiftOramMap();
	private Map<String, SwiftResourceHierarchy> resourceHierarchies = new ConcurrentHashMap<>();

	public SwiftResourceServlet()
	{
	}

	@Override
	protected void doHead(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		HTTPUtils.setContentType(servletRequest, servletResponse);
		HTTPUtils.setCharacterEncoding(servletResponse);
		HTTPUtils.setTransactionId(servletResponse);

		SwiftResourceHierarchy resourceHierarchy = getResourceHierarchy(servletRequest);
		List<String> resourcePath = getRequestedResourcePath(servletRequest);
		String resourceType = resourceHierarchy.getResourceType(resourcePath);

		if(resourceHierarchy.hasResource(resourcePath))
		{
			SwiftResource resource = resourceHierarchy.getResource(resourcePath);
			if(resource.getDeleteAt() != null && resource.getDeleteAt().before(new Date()))
			{
				doDelete(servletRequest, servletResponse);
				servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
				return;
			}

			HTTPUtils.setHeadersFromResource(servletResponse, resource);
			if(resourceType.equals("object"))
			{
				servletResponse.setContentLength(resource.getBytes());
				servletResponse.setStatus(HttpServletResponse.SC_OK);
			}
			else
			{
				servletResponse.setStatus(HttpServletResponse.SC_NO_CONTENT);
			}
			return;
		}

		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		HTTPUtils.setContentType(servletRequest, servletResponse);
		HTTPUtils.setCharacterEncoding(servletResponse);
		HTTPUtils.setTransactionId(servletResponse);

		SwiftResourceHierarchy resourceHierarchy = getResourceHierarchy(servletRequest);
		List<String> resourcePath = getRequestedResourcePath(servletRequest);
		String resourceType = resourceHierarchy.getResourceType(resourcePath);
		try
		{
			if(resourceHierarchy.hasResource(resourcePath))
			{
				SwiftResource resource = resourceHierarchy.getResource(resourcePath);

				if(resource.getDeleteAt() != null && resource.getDeleteAt().before(new Date()))
				{
					doDelete(servletRequest, servletResponse);
					servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
					return;
				}

				HTTPUtils.setHeadersFromResource(servletResponse, resource);
				if(resourceType.equals("object") && resource != null)
				{
					SwiftObject object = (SwiftObject) resource;
					String etagList = servletRequest.getHeader("If-None-Match");
					if(etagList != null && etagList.contains(object.getHash()))
					{
						servletResponse.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
						return;
					}
					if(object.getBytes() > 0)
					{
						startAsyncORAMRequests(servletRequest, servletResponse, object, null);
					}
					return;
				}else
				{
					SwiftResource filteredResource = filterResource(
							resource,
							servletRequest.getParameter("reverse"),
							servletRequest.getParameter("limit"),
							servletRequest.getParameter("marker"),
							servletRequest.getParameter("end_marker"),
							servletRequest.getParameter("prefix"),
							servletRequest.getParameter("delimiter")
					);
					String responseText = SerializationUtils.serializeSwiftResource(
							filteredResource,
							HTTPUtils.getRequestedResponseFormat(servletRequest)
					);
					servletResponse.getWriter().write(responseText);
					servletResponse.setStatus(HttpServletResponse.SC_OK);
					return;
				}
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
			servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}

		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Override
	protected void doPut(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		try
		{
			HTTPUtils.setContentType(servletRequest, servletResponse);
			HTTPUtils.setCharacterEncoding(servletResponse);
			HTTPUtils.setTransactionId(servletResponse);

			SwiftResourceHierarchy resourceHierarchy = getResourceHierarchy(servletRequest);
			List<String> resourcePath = getRequestedResourcePath(servletRequest);
			String resourceType = resourceHierarchy.getResourceType(resourcePath);
			String resourceName = resourcePath.get(resourcePath.size() - 1);

			if(resourceType.equals("object") && servletRequest.getHeader("X-Copy-From") != null)
			{
				//Copy of existing object
				String copyFrom =  servletRequest.getHeader("X-Copy-From");
				List<String> copyFromPath = new ArrayList<>();
				copyFromPath.add(resourcePath.get(0));
				copyFromPath.add(copyFrom.split("/")[0]);
				copyFromPath.add(copyFrom.split("/")[1]);
				if(resourceHierarchy.hasResource(copyFromPath))
				{
					SwiftResource copyFromResource = resourceHierarchy.getResource(copyFromPath);
					if(resourceHierarchy.hasResource(resourcePath))
					{
						resourceHierarchy.removeResource(resourcePath);
					}
					SwiftObject copiedResource = new SwiftObject((SwiftObject)copyFromResource);
					copiedResource.setName(resourceName);
					resourceHierarchy.addResource(resourcePath, copiedResource);
					for(String key : swiftOramMap.getOramKeys((SwiftObject) copyFromResource))
					{
						swiftOramMap.addOramKey(key, copiedResource);
					}
					boolean freshMetdata = servletRequest.getHeader("X-Fresh-Metadata") != null && Boolean.parseBoolean(servletRequest.getHeader("X-Fresh-Metadata"));
					if(freshMetdata)
					{
						copiedResource.clearMetadata();
					}
					servletResponse.setStatus(HttpServletResponse.SC_CREATED);
				}
			}
			else if(resourceType.equals("container") && !resourceHierarchy.hasResource(resourcePath))
			{
				resourceHierarchy.createResource(resourcePath);
				servletResponse.setStatus(HttpServletResponse.SC_CREATED);
			}
			else
			{
				//New object, data in request body
				byte[] data = new byte[0];
				data = MiscUtils.getInstance().ByteArrayFromInputStream(servletRequest.getInputStream());

				if(resourceType.equals("object"))
				{
					SwiftObject object = (SwiftObject) resourceHierarchy.getResource(resourcePath);
					if(object != null)
					{
						doDelete(servletRequest, servletResponse);
					}
					object = (SwiftObject) resourceHierarchy.createResource(resourcePath, data);
					if(data.length > 0)
					{
						startAsyncORAMRequests(servletRequest, servletResponse, object, data);
					}
					servletResponse.setStatus(HttpServletResponse.SC_CREATED);
				}else
				{
					servletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
				}
			}

			SwiftResource resource = resourceHierarchy.getResource(resourcePath);
			if(resource != null)
			{
				updateMetadataFromHeaders(servletRequest, resource);
				HTTPUtils.setHeadersFromResource(servletResponse, resource);
				String storagePolicy = servletRequest.getHeader("X-Storage-Policy");
				if(storagePolicy != null && !storagePolicy.isEmpty() && resource instanceof SwiftContainer)
				{
					((SwiftContainer) resource).setStoragePolicy(storagePolicy);
				}
				String contentType = servletRequest.getHeader("Content-Type");
				if(contentType != null && !contentType.isEmpty() && resource instanceof SwiftObject)
				{
					((SwiftObject) resource).setContentType(contentType);
				}
				return;
			}
		}
		catch(IllegalArgumentException e)
		{
			try {
				servletResponse.getWriter().write(e.getMessage());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			servletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		catch (IOException e)
		{
			servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}

		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Override
	protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		try
		{
			HTTPUtils.setContentType(servletRequest, servletResponse);
			HTTPUtils.setCharacterEncoding(servletResponse);
			HTTPUtils.setTransactionId(servletResponse);

			SwiftResourceHierarchy resourceHierarchy = getResourceHierarchy(servletRequest);
			List<String> resourcePath = getRequestedResourcePath(servletRequest);
			String resourceType = resourceHierarchy.getResourceType(resourcePath);

			if(resourceHierarchy.hasResource(resourcePath))
			{
				SwiftResource object = resourceHierarchy.getResource(resourcePath);
				updateMetadataFromHeaders(servletRequest, object);
				if(resourceType.equals("object"))
				{
					servletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
				}
				else
				{
					servletResponse.setStatus(HttpServletResponse.SC_NO_CONTENT);
				}
				return;
			}
		}
		catch (IllegalArgumentException e)
		{
			try {
				servletResponse.getWriter().write(e.getMessage());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			servletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}

		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Override
	protected void doDelete(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		try
		{
			HTTPUtils.setContentType(servletRequest, servletResponse);
			HTTPUtils.setCharacterEncoding(servletResponse);
			HTTPUtils.setTransactionId(servletResponse);

			SwiftResourceHierarchy resourceHierarchy = getResourceHierarchy(servletRequest);
			List<String> resourcePath = getRequestedResourcePath(servletRequest);
			String resourceType = resourceHierarchy.getResourceType(resourcePath);
			SwiftResource resource = resourceHierarchy.getResource(resourcePath);
			if(!resourceHierarchy.hasResource(resourcePath))
			{
				servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			else if(resource.getChildren().size() > 0)
			{

				servletResponse.getWriter().write("There was a conflict when trying to complete your request.");
				servletResponse.setStatus(HttpServletResponse.SC_CONFLICT);
				return;
			}
			if(resourceType.equals("object"))
			{
				swiftOramMap.removeSwiftObject((SwiftObject) resource);
			}
			resourceHierarchy.removeResource(resourcePath);
			servletResponse.setStatus(HttpServletResponse.SC_NO_CONTENT);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public void onRequestSuccess(Request request, DataItem dataItem)
	{
		AsyncContext ctxt = asyncContexts.get(request.getId());
		int idx = pendingOramRequests.get(ctxt).get(request);
		pendingOramRequests.get(ctxt).remove(request);
		if(completedOramRequests.get(ctxt) == null)
		{
			completedOramRequests.put(ctxt, new ConcurrentHashMap<>());
		}
		completedOramRequests.get(ctxt).put(idx, request);
		if(completedOramDataItems.get(ctxt) == null)
		{
			completedOramDataItems.put(ctxt, new ConcurrentHashMap<>());
		}
		completedOramDataItems.get(ctxt).put(idx, dataItem);
		int nextRequestIdx = nextOramRequest.get(ctxt);

		try
		{
			while(request.getType() == Request.RequestType.GET
					&& completedOramDataItems.get(ctxt).get(nextRequestIdx) != null)
			{
				log.append("[SwiftResourceServlet] Writing part " + nextRequestIdx + " of " + (nextRequestIdx + pendingOramRequests.get(ctxt).size()) + "(req key: " + request.getKey() + ")", Log.TRACE);

				GetRequest getRequest = (GetRequest) completedOramRequests.get(ctxt).get(nextRequestIdx);
				HttpServletResponse servletResponse = (HttpServletResponse) ctxt.getResponse();
				DataItem di = completedOramDataItems.get(ctxt).get(nextRequestIdx);
				VariableLengthDataItem varDataItem = new VariableLengthDataItem(di.getData());
				byte[] data = varDataItem.getData();
				int start = varDataItem.getStart();
				int length = Math.min(varDataItem.getLength(), data.length - start);
				if(getRequest.getStartOffset() != null)
				{
					start = getRequest.getStartOffset();
				}
				if(getRequest.getEndOffset() != null)
				{
					length = getRequest.getEndOffset() - start;
				}
				if(start + length > data.length) { Errors.error("Coding FAIL!"); }
				servletResponse.getOutputStream().write(data, start, length);
				completedOramRequests.get(ctxt).remove(nextRequestIdx);
				completedOramDataItems.get(ctxt).remove(nextRequestIdx);
				nextRequestIdx++;
			}
			nextOramRequest.put(ctxt, nextRequestIdx);


			if(pendingOramRequests.get(ctxt).size() % 127 == 0)
			{
				log.append("[SwiftResourceServlet] Pending requests " + pendingOramRequests.get(ctxt).size(), Log.TRACE);
			}

			if(pendingOramRequests.get(ctxt).size() == 0)
			{
				log.append("[SwiftResourceServlet] Completing req key: " + request.getKey(), Log.TRACE);
				ctxt.complete();
				completedOramRequests.remove(ctxt);
			}
		}
		catch(EofException e)
		{
			ctxt.complete();
		}
		catch(IllegalStateException e)
		{
			System.out.println("AsyncContext error, possible timeout " + e);
		}
		catch(IOException e)
		{
			HttpServletResponse servletResponse = (HttpServletResponse) ctxt.getResponse();
			servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			e.printStackTrace();
		}

		asyncContexts.remove(request.getId());

	}

	@Override
	public void onRequestFailure(Request request, DataItem dataItem)
	{
		System.out.println("Error in request " + request);
		AsyncContext ctxt = asyncContexts.get(request.getKey());
		pendingOramRequests.get(ctxt).remove(request);
		asyncContexts.remove(request.getKey());
		HttpServletResponse servletResponse = (HttpServletResponse) ctxt.getResponse();
		servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		ctxt.complete();
	}

	private void startAsyncORAMRequests(HttpServletRequest servletRequest, HttpServletResponse servletResponse, SwiftObject object, byte[] data) throws IOException
	{

		AsyncContext context = servletRequest.startAsync(servletRequest, servletResponse);
		context.setTimeout(SwiftConstants.ASYNC_TIMEOUT);
		if(pendingOramRequests.get(context) == null)
		{
			pendingOramRequests.put(context, new ConcurrentHashMap<>());
		}
		nextOramRequest.put(context, 0);

		List<Request> requests = new ArrayList<>();
		if(data == null)
		{
			List<String> keys = swiftOramMap.getOramKeys(object);
			int startBlock = getStartBlock(servletRequest, object);
			int endBlock =  getEndBlock(servletRequest, object);
			for(int i = startBlock; i <= endBlock; i++)
			{
				int start = 0;
				int end = VariableLengthDataItem.getMaxDataLength();
				if(i == startBlock)
				{
					start = getStartOffset(servletRequest, object) - i * VariableLengthDataItem.getMaxDataLength();
				}
				if(i == endBlock)
				{
					end = getEndOffset(servletRequest, object) - i * VariableLengthDataItem.getMaxDataLength();
				}
				Request req = new GetRequest(keys.get(i), start, end);
				requests.add(req);
			}
			log.append("[SwiftResourceServlet] Starting GET operation", Log.TRACE);
			log.append("startBlock:" + startBlock + " endBlock: " + endBlock, Log.TRACE);
		}
		else
		{
			int i = 0;
			for(int pos = 0; pos < data.length; pos += VariableLengthDataItem.getMaxDataLength())
			{
				int length = Math.min(data.length - pos, VariableLengthDataItem.getMaxDataLength());
				Request req = new PutRequest(swiftOramMap.getNextOramKey(), new VariableLengthDataItem(data, pos, length));
				requests.add(req);
				swiftOramMap.addOramKey(req.getKey(), object);
				i++;
			}
			log.append("[SwiftResourceServlet] Starting PUT operation", Log.TRACE);
		}
		try
		{
			for(int i = 0; i < requests.size(); i++)
			{
				Request req = requests.get(i);
				pendingOramRequests.get(context).put(req, i);
				asyncContexts.put(req.getId(), context);
				getRequestQueue().put(req);
			}
			log.append("[SwiftResourceServlet] " + pendingOramRequests.get(context).size() + " requests queued", Log.TRACE);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	private SwiftResource filterResource(SwiftResource resource, String reverse, String limit, String marker, String endMarker, String prefix, String delimiter)
	{
		SwiftResource result = resource;

		if(reverse != null || limit != null || marker != null || endMarker != null || prefix != null || delimiter != null)
		{
			if(resource instanceof SwiftAccount)
			{
				result = new SwiftAccount((SwiftAccount) resource);
			}
			else if(resource instanceof SwiftContainer)
			{
				result = new SwiftContainer((SwiftContainer) resource);
			}
			if(reverse != null && Boolean.parseBoolean(reverse))
			{
				result.setChildren(result.getChildren().descendingMap());
			}
			if(marker != null && !marker.isEmpty())
			{
				for(String name : result.getChildren().keySet())
				{
					if(name.compareTo(marker) < 0 || name.compareTo(marker) == 0)
					{
						result.removeChild(name);
					}
				}
			}
			if(endMarker != null && !endMarker.isEmpty())
			{
				for(String name : result.getChildren().keySet())
				{
					if(name.compareTo(endMarker) > 0 || name.compareTo(endMarker) == 0)
					{
						result.removeChild(name);
					}
				}
			}
			if(prefix != null && !prefix.isEmpty())
			{
				for(String name : result.getChildren().keySet())
				{
					if(!name.startsWith(prefix))
					{
						result.removeChild(name);
					}
				}
			}
			if(delimiter != null && !delimiter.isEmpty())
			{
				for(String name : result.getChildren().keySet())
				{
					if(name.contains(delimiter))
					{
						result.removeChild(name);
						result.addChild(new SwiftDirectory(name.split(delimiter)[0] + delimiter));
					}
				}
			}
			if(limit != null && !limit.isEmpty())
			{
				int n = Integer.parseInt(limit);
				while(result.getChildren().size() > n)
				{
					String name = result.getChildren().keySet().iterator().next();
					result.removeChild(name);
				}
			}
		}

		return result;
	}

	private List<String> getRequestedResourcePath(HttpServletRequest servletRequest)
	{
		List<String> resources = new ArrayList<String>();
		String path = servletRequest.getRequestURI();
		String[] parts = path.split("/", 5);
		if(path.startsWith("/v1/")) {
			try {
				if(!parts[2].isEmpty())
				{
					resources.add(parts[2]);
				}
				if(!parts[3].isEmpty())
				{
					resources.add(parts[3]);
				}
				if(!parts[4].isEmpty())
				{
					resources.add(parts[4]);
				}
			} catch (IndexOutOfBoundsException e) {}
		}
		return resources;
	}

	private SwiftResourceHierarchy getResourceHierarchy(HttpServletRequest servletRequest)
	{
		String token = servletRequest.getHeader("X-Auth-Token");
		if(token == null)
		{
			token = "";
		}
		if(!resourceHierarchies.containsKey(token))
		{
			SwiftResourceHierarchy resourceHierarchy = new SwiftResourceHierarchy();
			resourceHierarchy.createResource(Collections.singletonList(SwiftConstants.DEFAULT_ACCOUNT));
			resourceHierarchies.put(token, resourceHierarchy);
		}
		return resourceHierarchies.get(token);
	}

	public int getStartBlock(HttpServletRequest servletRequest, SwiftResource object)
	{
		return getStartOffset(servletRequest, object) / VariableLengthDataItem.getMaxDataLength();
	}

	public int getEndBlock(HttpServletRequest servletRequest, SwiftResource object)
	{
		return getEndOffset(servletRequest, object) / VariableLengthDataItem.getMaxDataLength();
	}

	public int getStartOffset(HttpServletRequest servletRequest, SwiftResource object)
	{
		String rangeHeader = servletRequest.getHeader("Range");
		if(rangeHeader != null)
		{
			String range = rangeHeader.replace("bytes=","");
			if(range.contains(","))
			{
				throw new NotImplementedException();
			}
			String start = range.split("-")[0];
			String end = range.split("-")[1];

			if(start.length() == 0)
			{
				return object.getBytes() - Integer.parseInt(end);
			}

			return Integer.parseInt(start);
		}
		return 0;
	}

	public int getEndOffset(HttpServletRequest servletRequest, SwiftResource object)
	{
		String rangeHeader = servletRequest.getHeader("Range");
		if(rangeHeader != null)
		{
			String range = rangeHeader.replace("bytes=","");
			if(range.contains(","))
			{
				throw new NotImplementedException();
			}
			String start = range.split("-")[0];
			String end = range.split("-")[1];

			if(start.length() == 0)
			{
				return object.getBytes();
			}
			if(end.length() == 0)
			{
				return object.getBytes();
			}

			return Integer.parseInt(end);
		}

		return object.getBytes();
	}

	private void updateMetadataFromHeaders(HttpServletRequest servletRequest, SwiftResource object)
	{
		updateCustomMetadataFromHeaders(servletRequest, object);
		updateSystemMetadataFromHeaders(servletRequest, object);
	}

	private void updateCustomMetadataFromHeaders(HttpServletRequest servletRequest, SwiftResource object)
	{
		for (Enumeration<String> en = servletRequest.getHeaderNames(); en.hasMoreElements(); )
		{
			String headerName = en.nextElement();
			if(headerName.matches("X-Remove-.*-Meta-.*"))
			{
				String key = headerName.replaceAll("X-Remove-.*-Meta-", "");
				object.removeMetadataValue(key);
			}
			else if(headerName.matches("X-.*-Meta-.*"))
			{
				String key = headerName.replaceAll("X-.*-Meta-", "").replace("_", "-");
				String value = servletRequest.getHeader(headerName);
				if(!key.isEmpty() && !value.isEmpty())
				{
					object.setMetadataValue(key, value);
				}
				else if(!key.isEmpty() && !value.isEmpty() && !object.hasMetadataKey(key) && object instanceof SwiftAccount)
				{
					object.setMetadataValue(key, value);
				}
				else if(!key.isEmpty() && !object.hasMetadataKey(key) && object instanceof SwiftObject)
				{
					object.setMetadataValue(key, value);
				}
				else if(value.isEmpty() && object.hasMetadataKey(key))
				{
					object.removeMetadataValue(key);
				}
			}
		}
	}

	private void updateSystemMetadataFromHeaders(HttpServletRequest servletRequest, SwiftResource object)
	{
		if(servletRequest.getHeader("X-Delete-After") != null)
		{
			String deleteAfter = servletRequest.getHeader("X-Delete-After");
			Date date = new Date();
			date.setTime(date.getTime() + Integer.parseInt(deleteAfter) * 1000);
			object.setDeleteAt(date);
		}
		else if(servletRequest.getHeader("X-Delete-At") != null)
		{
			String deleteAt = servletRequest.getHeader("X-Delete-At");
			object.setDeleteAt(new Date(Long.parseLong(deleteAt) * 1000));
		}
		for(Enumeration<String> en = servletRequest.getHeaderNames(); en.hasMoreElements(); )
		{
			String headerName = en.nextElement();
			if(SwiftResource.SYSTEM_METADATA_KEYS.stream().anyMatch(str -> str.toLowerCase().equals(headerName.toLowerCase())))
			{
				object.setMetadataValue(headerName, servletRequest.getHeader(headerName));
			}
		}

	}
}
