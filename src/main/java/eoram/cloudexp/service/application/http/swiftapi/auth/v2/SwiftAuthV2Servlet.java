package eoram.cloudexp.service.application.http.swiftapi.auth.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import eoram.cloudexp.service.application.http.swiftapi.SwiftConstants;
import eoram.cloudexp.service.application.http.swiftapi.utils.HTTPUtils;
import eoram.cloudexp.utils.MiscUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class SwiftAuthV2Servlet extends HttpServlet
{
	private ObjectMapper objectMapper = new ObjectMapper();

	public SwiftAuthV2Servlet()
	{
		objectMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.setDateFormat(new SimpleDateFormat("Y-MM-dd'T'HH:mm:ss'Z'"));
	}

	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		HTTPUtils.setContentType(servletRequest, servletResponse, "json");
		try
		{
			String path = servletRequest.getRequestURI();
			if(path.equals("/auth/v2.0"))
			{
				SwiftVersion ver = new SwiftVersion();
				ver.addLink("self", servletRequest.getScheme()
								+ "://" + servletRequest.getServerName()
								+ ":" + servletRequest.getServerPort()
								+ path
				);
				objectMapper.writeValue(servletResponse.getWriter(), ver);
				servletResponse.setStatus(HttpServletResponse.SC_OK);
				return;
			}
			else if(path.equals("/auth/v2.0/tenants"))
			{
				objectMapper.disable(SerializationFeature.WRAP_ROOT_VALUE);
				objectMapper.writeValue(servletResponse.getWriter(), new SwiftTenantList());
				objectMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
				servletResponse.setStatus(HttpServletResponse.SC_OK);
				return;
			}
			else if(path.equals("/auth/v2.0/extensions"))
			{
				objectMapper.writeValue(servletResponse.getWriter(), new SwiftExtensionList());
				servletResponse.setStatus(HttpServletResponse.SC_OK);
				return;
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
			servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}

		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Override
	protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		HTTPUtils.setContentType(servletRequest, servletResponse, "json");
		try
		{
			String path = servletRequest.getRequestURI();
			if(path.equals("/auth/v2.0/tokens"))
			{
				String storageUrl = servletRequest.getScheme()
						+ "://" + servletRequest.getServerName()
						+ ":" + servletRequest.getServerPort()
						+ "/v1/" + SwiftConstants.DEFAULT_ACCOUNT;
				String authUrl = servletRequest.getScheme()
						+ "://" + servletRequest.getServerName()
						+ ":" + servletRequest.getServerPort()
						+ "/auth/v2.0";
				byte[] data = MiscUtils.getInstance().ByteArrayFromInputStream(servletRequest.getInputStream());
				String user = objectMapper.readTree(data).path("auth").path("passwordCredentials").path("username").textValue();
				String token = MiscUtils.getInstance().getMD5Hash(user);
				SwiftAccess access = new SwiftAccess(token, storageUrl, authUrl);
				objectMapper.writeValue(servletResponse.getWriter(), access);
				servletResponse.setStatus(HttpServletResponse.SC_OK);
				return;
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
			servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}

		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}
}
