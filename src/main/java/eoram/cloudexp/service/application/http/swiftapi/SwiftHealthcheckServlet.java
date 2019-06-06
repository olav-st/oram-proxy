package eoram.cloudexp.service.application.http.swiftapi;

import eoram.cloudexp.service.application.http.swiftapi.utils.HTTPUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class SwiftHealthcheckServlet extends HttpServlet
{
	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		try
		{
			HTTPUtils.setTransactionId(servletResponse);
			servletResponse.getWriter().write("OK");
			servletResponse.setContentType("text/plain");
			servletResponse.setStatus(HttpServletResponse.SC_OK);
			return;
		}
		catch (IOException e)
		{
			e.printStackTrace();

		}
		servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}
}
