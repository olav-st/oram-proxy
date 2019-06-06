package eoram.cloudexp.construction;

import eoram.cloudexp.data.encoding.DefaultHeader;
import eoram.cloudexp.data.encoding.FrameworkHeader;
import eoram.cloudexp.data.encoding.Header;
import eoram.cloudexp.data.encoding.ObliviStoreHeader;
import eoram.cloudexp.implementation.*;
import eoram.cloudexp.interfaces.*;
import eoram.cloudexp.schemes.*;
import eoram.cloudexp.utils.Errors;

/**
 * Allows to instantiate the various client and storage schemes. 
 * The factory works by passing scheme's names (and possibly some parameters) to the {@link createClient} and {@link createStorage} methods
 * 
 * <p>
 * <p>
 * <h4>Implementation notes:</h4>
 * <ul>
 * <li>Uses the <a href="https://en.wikipedia.org/wiki/Singleton_pattern">Singleton</a> design pattern.</li>
 * </ul>
 * <p>
 */
public class SchemeFactory 
{
	private static SchemeFactory instance = null;	
	private SchemeFactory() {}
	
	public static synchronized SchemeFactory getInstance() 
	{ 
		if(instance == null) { instance = new SchemeFactory(); }
		return instance;
	}
	
	protected String[] parseDesc(String desc)
	{
		String d = null; String[] s = null;
		
		int pos = desc.indexOf("(");
		if(pos >= 0)
		{
			d = desc.substring(0, pos);
			String param = desc.substring(pos+1);
			param = param.substring(0, param.indexOf(")")); 
			s = param.split(",\\s*");
			desc = desc.substring(0, desc.indexOf("("));
		}
		else { d = desc; }
		
		String[] ret = new String[1 + ((s == null) ? 0 : s.length)];
		ret[0] = d; if(s != null) { for(int i=0; i<s.length; i++) { ret[1+i] = s[i]; } }
		return ret;
	}
	
	public ExternalStorageInterface createStorage(String storage, boolean reset)
	{
		String[] storageDesc = parseDesc(storage);
		
		InternalStorageInterface s = null;
		switch(storageDesc[0])
		{
		case "SyncLocal": 
			if(storageDesc.length > 1) 
			{
				Errors.verify(storageDesc.length == 2);
				s = new LocalStorage(storageDesc[1], reset); 
			}
			else { s = new LocalStorage(reset); }
			break;
		case "AsyncLocal":
			if(storageDesc.length > 2)
			{
				Errors.verify(storageDesc.length == 3);
				s = new AsyncLocalStorage(storageDesc[1], reset, Integer.parseInt(storageDesc[2])); 
			}
			else if(storageDesc.length > 1) 
			{
				Errors.verify(storageDesc.length == 2);
				s = new AsyncLocalStorage(storageDesc[1], reset); 
			}
			else { s = new AsyncLocalStorage(reset); }
			break;
		case "SyncInMemory": s = new InMemoryStorage(reset); break;
		case "AsyncInMemory": s = new AsyncInMemoryStorage(reset); break;
		case "S3": s = new AmazonS3Storage(reset); break;
		case "AsyncS3": s = new AmazonS3AsyncStorage(reset); break;
		case "Swift": s = new SwiftStorage(reset); break;
		case "AsyncSwift": s = new SwiftAsyncStorage(reset); break;
		default: 
			Errors.error("Unknown storage: " + storage);
		}
		
		return new StorageAdapter(s);
	}
	
	public ExternalClientInterface createClient(String client)
	{		
		String[] clientDesc = parseDesc(client);
		
		InternalClientInterface c = null;
		switch(clientDesc[0])
		{
		case "RAM": 
			
			if(clientDesc.length > 1)  
			{
				Errors.verify(clientDesc.length == 2);
				c = new RAMClient(Boolean.parseBoolean(clientDesc[1])); 
			}
			else { c = new RAMClient(); }
			break;
		case "LayeredORAM":
		case "SimHierarchical":
			c = new LayeredORAMClient();
			break;
		case "PracticalOS":
			c = new PracticalOSClient();
			break;
		case "PathORAM": 
			assert(clientDesc.length == 1);
			c = new PathORAMClient();
			break;
		case "RingORAM":
			assert(clientDesc.length == 1 || clientDesc.length == 2);
			if(clientDesc.length == 1) { c = new RingORAMClient(); }
			else { c = new RingORAMClient(Boolean.parseBoolean(clientDesc[1])); }
			break;
		case "ObliviStore":
			assert(clientDesc.length == 1 || clientDesc.length == 2);
			if(clientDesc.length == 1) { c = new ObliviStoreClient(); }
			else { c = new ObliviStoreClient(Integer.parseInt(clientDesc[1])); }
			break;
		case "OptFC":
		case "OptimizedFC":
		case "TreeBasedFC":
		{
			int m = Integer.parseInt(clientDesc[1]);
			int k = Integer.parseInt(clientDesc[2]);
			int z = Integer.parseInt(clientDesc[3]);
			int mpt = Integer.parseInt(clientDesc[4]);
			String sgy = clientDesc[5];
			
			
			assert(clientDesc.length == 6 || clientDesc.length == 10);
			if(clientDesc.length == 6)
			{ 
				c = new TreeBasedFrameworkClient(m, k, z, mpt, sgy); 
			}
			else 
			{ 
				boolean reliable = Integer.parseInt(clientDesc[6]) != 0;
				boolean fastCompletion = Integer.parseInt(clientDesc[7]) != 0;
				boolean randomPrefix = Integer.parseInt(clientDesc[8]) != 0;
				boolean uetp = Integer.parseInt(clientDesc[9]) != 0;
				
				c = new TreeBasedFrameworkClient(m, k, z, mpt, sgy, reliable, fastCompletion, randomPrefix, uetp); 
			}
			break;
		}
		default: 
			Errors.error("Unknown client: " + client);
		}
		
		return new ClientAdapter(c);
	}
	
	public void setHeader(String client)
	{
		String[] clientDesc = parseDesc(client);
		if(clientDesc[0].equalsIgnoreCase("ObliviStore")) 
		{
			Header.setCurrentHeader(new ObliviStoreHeader(0));
		}
		else if(clientDesc[0].contains("FC") || clientDesc[0].contains("Framework"))
		{
			Header.setCurrentHeader(new FrameworkHeader());
		}
		else
		{
			Header.setCurrentHeader(new DefaultHeader());
		}
	}
}
