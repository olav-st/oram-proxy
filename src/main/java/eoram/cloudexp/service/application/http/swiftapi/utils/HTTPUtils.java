package eoram.cloudexp.service.application.http.swiftapi.utils;

import eoram.cloudexp.service.application.http.swiftapi.resources.SwiftAccount;
import eoram.cloudexp.service.application.http.swiftapi.resources.SwiftContainer;
import eoram.cloudexp.service.application.http.swiftapi.resources.SwiftObject;
import eoram.cloudexp.service.application.http.swiftapi.resources.SwiftResource;
import eoram.cloudexp.utils.MiscUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class HTTPUtils
{
	public static void setContentType(HttpServletRequest servletRequest, HttpServletResponse servletResponse, String defaultFormat)
	{
		String format = HTTPUtils.getRequestedResponseFormat(servletRequest, defaultFormat);
		if(format.equals("xml"))
		{
			servletResponse.setContentType("application/xml");
		}
		else if(format.equals("json"))
		{
			servletResponse.setContentType("application/json");
		}
		else
		{
			servletResponse.setContentType("text/plain");
		}
	}

	public static void setContentType(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		setContentType(servletRequest, servletResponse, "text");
	}

	public static void setCharacterEncoding(HttpServletResponse servletResponse)
	{
		servletResponse.setCharacterEncoding("UTF-8");
	}

	public static void setTransactionId(HttpServletResponse servletResponse)
	{
		String txId = "tx" + MiscUtils.getInstance().getRandomHexString(21) + "-" + MiscUtils.getInstance().getRandomHexString(10);
		servletResponse.setHeader("X-Trans-Id", txId);
		servletResponse.setHeader("X-Openstack-Request-Id", txId);
	}

	public static void setHeadersFromResource(HttpServletResponse servletResponse, SwiftResource resource)
	{
		servletResponse.setHeader("Accept-Ranges", "bytes");
		servletResponse.setHeader("X-Timestamp", "" + resource.getCreatedAt().getTime() / 1000);
		if(resource.getDeleteAt() != null)
		{
			servletResponse.setHeader("X-Delete-At", "" + resource.getDeleteAt().getTime() / 1000);
		}
		if(resource instanceof SwiftAccount)
		{
			servletResponse.setHeader("X-Account-Bytes-Used", "" + resource.getBytes());
			servletResponse.setHeader("X-Account-Container-Count", "" + resource.getCount());
			int objectCount = resource.getChildren().values().stream().map(SwiftResource::getCount).reduce(0, (x,y) -> x + y);
			servletResponse.setHeader("X-Account-Object-Count", "" + objectCount);
			setMetadataHeaders(servletResponse, resource, "Account");
		}
		else if(resource instanceof SwiftContainer)
		{
			servletResponse.setHeader("X-Container-Object-Count", "" + resource.getCount());
			servletResponse.setHeader("X-Container-Bytes-Used", "" + resource.getBytes());
			if(!((SwiftContainer)resource).getStoragePolicy().isEmpty())
			{
				servletResponse.setHeader("X-Storage-Policy", ((SwiftContainer)resource).getStoragePolicy());
			}
			setMetadataHeaders(servletResponse, resource, "Container");
		}
		else if(resource instanceof SwiftObject)
		{
			servletResponse.setHeader("Etag", ((SwiftObject)resource).getHash().toLowerCase());
			SimpleDateFormat sdf = new SimpleDateFormat("E, dd MMM YYYY HH:mm:ss z");
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			servletResponse.setHeader("Last-Modified", sdf.format(resource.getLastModified()));
			setMetadataHeaders(servletResponse, resource, "Object");
		}
	}

	private static void setMetadataHeaders(HttpServletResponse servletResponse, SwiftResource resource, String type) {
		for(String key : resource.getMetadataKeys())
		{
			String value = resource.getMetadataValue(key);
			if(SwiftResource.SYSTEM_METADATA_KEYS.stream().anyMatch(str -> str.toLowerCase().equals(key.toLowerCase())))
			{
				servletResponse.setHeader(key, value);
			}
			else
			{
				servletResponse.setHeader("X-" + type + "-Meta-" + key, value);
			}
		}
	}

	public static String getRequestedResponseFormat(HttpServletRequest servletRequest, String defaultFormat)
	{
		String format = defaultFormat;
		String acceptHeaderValue = servletRequest.getHeader("Accept");
		if(acceptHeaderValue != null && !acceptHeaderValue.isEmpty())
		{
			if(acceptHeaderValue.contains("json"))
			{
				format = "json";
			}
			else if(acceptHeaderValue.contains("xml"))
			{
				format = "xml";
			}
		}
		String[] formatParameterValues = servletRequest.getParameterValues("format");
		if(formatParameterValues != null && formatParameterValues.length > 0)
		{
			format = formatParameterValues[formatParameterValues.length - 1];
		}
		return format;
	}

	public static String getRequestedResponseFormat(HttpServletRequest servletRequest)
	{
		return getRequestedResponseFormat(servletRequest, "text");
	}
}
