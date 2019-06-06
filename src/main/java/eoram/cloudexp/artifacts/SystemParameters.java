package eoram.cloudexp.artifacts;

import java.io.File;

/**
 * Represents the parameters of the system. 
 * 
 * <p>
 * <p>
 * <h4>Implementation notes:</h4>
 * <ul>
 * <li>Uses the <a href="https://en.wikipedia.org/wiki/Singleton_pattern">Singleton</a> design pattern.</li>
 * </ul>
 * <p>
 * 
 * @see ClientParameters, SessionState
 * 
 */
public class SystemParameters 
{
	private SystemParameters() {}
	
	private static final SystemParameters instance = new SystemParameters();
	public static SystemParameters getInstance() { return instance; }
	
	public String logsDirFP = "./log";

	public File credentials = new File("./credentials.file");
	
	public String localDirectoryFP = "./local";
	public String tempDirectoryFP = "./temp";
	
	public int storageOpMaxAttempts = 4;
	public int clientReqMaxAttempts = 2;
}
