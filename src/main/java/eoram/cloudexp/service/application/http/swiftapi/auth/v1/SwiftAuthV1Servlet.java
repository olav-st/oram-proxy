package eoram.cloudexp.service.application.http.swiftapi.auth.v1;

import eoram.cloudexp.service.application.http.swiftapi.SwiftConstants;
import eoram.cloudexp.utils.MiscUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SwiftAuthV1Servlet extends HttpServlet
{
	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		String path = servletRequest.getRequestURI();
		if(path.equals("/auth/v1.0"))
		{
			String storageUrl = servletRequest.getScheme()
					+ "://" + servletRequest.getServerName()
					+ ":" + servletRequest.getServerPort()
					+ "/v1/" + SwiftConstants.DEFAULT_ACCOUNT;
			String user = servletRequest.getHeader("X-Auth-User");
			String token = "AUTH_tk" + MiscUtils.getInstance().getMD5Hash(user);
			servletResponse.setHeader("X-Storage-Url", storageUrl);
			servletResponse.setHeader("X-Auth-Token", token);
			servletResponse.setStatus(HttpServletResponse.SC_OK);
			return;
		}
		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}
}
