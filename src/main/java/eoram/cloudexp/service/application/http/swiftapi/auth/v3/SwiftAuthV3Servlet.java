package eoram.cloudexp.service.application.http.swiftapi.auth.v3;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SwiftAuthV3Servlet extends HttpServlet
{
	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}

	@Override
	protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		servletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
	}
}
