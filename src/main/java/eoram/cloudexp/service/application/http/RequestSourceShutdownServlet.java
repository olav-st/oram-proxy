package eoram.cloudexp.service.application.http;

import eoram.cloudexp.service.application.RequestSource;
import eoram.cloudexp.service.application.http.swiftapi.utils.HTTPUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RequestSourceShutdownServlet extends HttpServlet
{
	private RequestSource rs;

	public RequestSourceShutdownServlet(RequestSource rs)
	{
		this.rs = rs;
	}

	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		rs.close();
		servletResponse.setStatus(HttpServletResponse.SC_ACCEPTED);
	}
}
