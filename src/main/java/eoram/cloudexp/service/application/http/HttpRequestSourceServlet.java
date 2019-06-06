package eoram.cloudexp.service.application.http;

import eoram.cloudexp.data.DataItem;
import eoram.cloudexp.service.Request;

import javax.servlet.http.HttpServlet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract class for handling HTTP requests.
 * Provides a BlockingQueue of 'Request' objects
 *
 */
public abstract class HttpRequestSourceServlet extends HttpServlet
{
    private BlockingQueue<Request> requestQueue = new LinkedBlockingQueue<Request>();

    public abstract void onRequestSuccess(Request request, DataItem dataItem);

    public abstract void onRequestFailure(Request request, DataItem dataItem);

    public BlockingQueue<Request> getRequestQueue()
    {
        return requestQueue;
    }
}
