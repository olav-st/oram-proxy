package eoram.cloudexp.service.application;

import eoram.cloudexp.interfaces.CompletionCallback;
import eoram.cloudexp.service.Request;
import eoram.cloudexp.service.ScheduledRequest;
import eoram.cloudexp.service.application.http.HttpRequestSourceServlet;
import eoram.cloudexp.service.application.http.RequestSourceShutdownServlet;
import eoram.cloudexp.service.application.http.swiftapi.SwiftHealthcheckServlet;
import eoram.cloudexp.service.application.http.swiftapi.SwiftInfoServlet;
import eoram.cloudexp.service.application.http.swiftapi.SwiftResourceServlet;
import eoram.cloudexp.service.application.http.swiftapi.auth.v1.SwiftAuthV1Servlet;
import eoram.cloudexp.service.application.http.swiftapi.auth.v2.SwiftAuthV2Servlet;
import eoram.cloudexp.service.application.http.swiftapi.auth.v3.SwiftAuthV3Servlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.concurrent.TimeUnit;

/**
 * Implements a request source backed by an HTTP server listening on a port.
 *
 */
public class HTTPServerRequestSource extends RequestSource implements CompletionCallback
{
	private Server server;
	private HttpRequestSourceServlet servlet;

	public HTTPServerRequestSource(short portNumber) throws Exception {
		server = new Server(portNumber);
		servlet = new SwiftResourceServlet();
		ServletHolder asyncHolder = new ServletHolder(servlet);
		asyncHolder.setAsyncSupported(true);
		ServletContextHandler context = new ServletContextHandler();
		context.setContextPath("/");
		context.addServlet(asyncHolder, "/*");
		context.addServlet(new ServletHolder(new SwiftInfoServlet()), "/info");
		context.addServlet(new ServletHolder(new SwiftHealthcheckServlet()), "/healthcheck");
		context.addServlet(new ServletHolder(new SwiftAuthV1Servlet()), "/auth/v1.0/*");
		context.addServlet(new ServletHolder(new SwiftAuthV2Servlet()), "/auth/v2.0/*");
		context.addServlet(new ServletHolder(new SwiftAuthV3Servlet()), "/auth/v3.0/*");
		context.addServlet(new ServletHolder(new RequestSourceShutdownServlet(this)), "/shutdown");
		server.setHandler(context);
		server.start();

		//Shutdown hook to stop server in case of CTRL-C or SIGTERM
		final Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			close();
			try {
				mainThread.join(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}));
	}

	@Override
	public void close()
	{
		try
		{
			server.stop();
		}
		catch (Exception ignored) {}
		super.close();
	}

	@Override
	public boolean hasNext()
	{
		while(server.isRunning())
		{
			try {
				Request req = servlet.getRequestQueue().poll(1, TimeUnit.SECONDS);
				if (req != null) {
					requests.put(nextReqId, req);
					return true;
				}
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		return false;
	}

	@Override
	public Request next()
	{
		if(nextReqId == -1)
		{
			rewind();
		}
		return requests.get(nextReqId++);
	}

	@Override
	public void onSuccess(ScheduledRequest scheduled)
	{
		servlet.onRequestSuccess(scheduled.getRequest(), scheduled.getDataItem());
		//cleanup
		requests.remove(scheduled.getRequest().getId());
	}

	@Override
	public void onFailure(ScheduledRequest scheduled)
	{
		servlet.onRequestFailure(scheduled.getRequest(), scheduled.getDataItem());
		//cleanup
		requests.remove(scheduled.getRequest().getId());
	}
}
