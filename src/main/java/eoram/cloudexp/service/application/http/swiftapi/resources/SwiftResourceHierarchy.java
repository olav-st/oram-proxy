package eoram.cloudexp.service.application.http.swiftapi.resources;

import java.util.Date;
import java.util.List;

public class SwiftResourceHierarchy
{
	private SwiftResource root = new SwiftResource("root");

	public SwiftResource createResource(List<String> path)
	{
		return createResource(path, null);
	}

	public SwiftResource createResource(List<String> path, byte[] data)
	{
		String name = getResourceName(path);
		String type = getResourceType(path);
		SwiftResource parent = getParentResource(path);
		SwiftResource child = null;
		switch (type)
		{
			case "account":
				child = new SwiftAccount(name);
				break;
			case "container":
				child = new SwiftContainer(name);
				break;
			case "object":
				child = new SwiftObject(name, data);
				break;
		}
		parent.addChild(child);
		return child;
	}

	public void addResource(List<String> path, SwiftResource resource)
	{
		SwiftResource parent = getParentResource(path);
		parent.addChild(resource);
	}

	public void removeResource(List<String> path)
	{
		SwiftResource parent = getParentResource(path);
		String name = getResourceName(path);
		parent.removeChild(name);
	}

	public SwiftResource getResource(List<String> path)
	{
		SwiftResource resource = null;

		if(path.size() > 0 && root.hasChild(path.get(0)))
		{
			resource = root.getChild(path.get(0));
			for(int i = 1; i < path.size(); i++)
			{
				if(path.get(i) != null && resource != null)
				{
					resource = resource.getChild(path.get(i));
				}
			}
		}

		return resource;
	}

	public SwiftResource getParentResource(List<String> path)
	{
		if(path.size() == 0)
		{
			return null;
		}
		else if(path.size() == 1)
		{
			return root;
		}
		return getResource(path.subList(0, path.size() -1));
	}

	public boolean hasResource(List<String> path)
	{
		return getResource(path) != null;
	}

	public SwiftResource getRoot()
	{
		return root;
	}

	public static String getResourceType(List<String> path)
	{
		switch(path.size())
		{
			case 0:
				return "root";
			case 1:
				return "account";
			case 2:
				return "container";
			case 3:
				return "object";
			default:
				throw new IllegalArgumentException("path.size() > 3");
		}
	}

	public static String getResourceName(List<String> path)
	{
		if(path.size() == 0)
		{
			return null;
		}
		return path.get(path.size() -1);
	}
}
