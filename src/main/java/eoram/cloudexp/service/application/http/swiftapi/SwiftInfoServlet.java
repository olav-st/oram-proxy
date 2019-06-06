package eoram.cloudexp.service.application.http.swiftapi;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class SwiftInfoServlet extends HttpServlet
{
	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
	{
		try
		{
			objectMapper.writeValue(servletResponse.getWriter(), new SwiftInfo());
			servletResponse.setContentType("application/json");
			servletResponse.setStatus(HttpServletResponse.SC_OK);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	}
}
