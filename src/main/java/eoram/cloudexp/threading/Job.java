package eoram.cloudexp.threading;

import java.util.concurrent.Callable;

import eoram.cloudexp.pollables.Pollable;
import eoram.cloudexp.utils.Errors;

/**
 * Represents a job.
 *
 * @param <T>
 */
public class Job<T> extends Pollable
{
	protected String name = null;
	protected Callable<T> task = null;
	protected T result = null;
	protected boolean done = false;
	
	public Job(String n, Callable<T> t) { name = n; task = t; Errors.verify(task != null); }
	public Job(Callable<T> t) { this("unnamed-job", t); }

	public void call() 
	{
		try { result = task.call(); } 
		catch (Exception e) 
		{ 
			done = true;
			e.printStackTrace();
			System.exit(-1);
			Errors.error(e);
		}
		done = true;
	}	
	
	public String toString() { return name + ": " + task.toString(); }
	
	@Override
	public boolean isReady() { return done == true; }	
}
