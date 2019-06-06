package eoram.cloudexp.service.runners;

import eoram.cloudexp.interfaces.ExternalClientInterface;
import eoram.cloudexp.interfaces.ExternalStorageInterface;

/** Unsafe ParallelRunner **/
public class InitRunner extends ParallelRunner 
{
	public InitRunner(ExternalClientInterface c, ExternalStorageInterface s)  { super(c, s, false); }
}
