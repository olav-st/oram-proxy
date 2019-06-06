package eoram.cloudexp.service.application.http.swiftapi.resources;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import eoram.cloudexp.service.application.http.swiftapi.SwiftConstants;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class SwiftResource implements Cloneable
{
	public static final List<String> SYSTEM_METADATA_KEYS = Arrays.asList(
			"content-type", "content-encoding",
			"content-disposition", "x-delete-at"
	);

	private Long id;
	private static AtomicLong nextResourceId = new AtomicLong(1);
	private Map<String, String> metadata = new ConcurrentHashMap<>();
	private Date createdAt = new Date();
	private Date deleteAt = null;
	private NavigableMap<String, SwiftResource> children = new ConcurrentSkipListMap<>();

	@JsonProperty
	protected String name;
	@JsonProperty
	protected int count = 0;
	@JsonProperty
	protected Date last_modified = new Date();

	public SwiftResource(String name)
	{
		this.id = nextResourceId.getAndIncrement();
		this.name = name;
	}

	public SwiftResource(SwiftResource other)
	{
		this.id = other.getId();
		this.name = other.getName();
		this.createdAt = other.getCreatedAt();
		this.deleteAt = other.getDeleteAt();
		this.count = other.getCount();
		this.last_modified = other.getLastModified();
		this.children = new ConcurrentSkipListMap<>(other.getChildren());
		this.metadata = new ConcurrentSkipListMap<>(other.getMetadata());
	}

	public void addChild(SwiftResource child)
	{
		children.put(child.getName(), child);
		count++;
	}

	public void removeChild(String name)
	{
		if(hasChild(name))
		{
			SwiftResource resource = getChild(name);
			count--;
			children.remove(name);
		}
	}

	public void renameChild(String srcName, String destName)
	{
		SwiftResource resource = getChild(srcName);
		removeChild(srcName);
		resource.setName(destName);
		addChild(resource);
	}

	public boolean hasChild(String name)
	{
		return children.containsKey(name);
	}

	public SwiftResource getChild(String name)
	{
		if(hasChild(name))
		{
			return children.get(name);
		}
		return null;
	}

	public void setChildren(NavigableMap<String, SwiftResource> children)
	{
		this.children = children;
	}

	public void setMetadataValue(String key, String value)
	{
		if(key.length() > SwiftConstants.MAX_META_NAME_LENGTH)
		{
			throw new IllegalArgumentException("Metadata name too long");
		}
		if(value.length() > SwiftConstants.MAX_META_VALUE_LENGTH)
		{
			throw new IllegalArgumentException("Metadata value longer than " + SwiftConstants.MAX_META_VALUE_LENGTH);
		}
		if(metadata.size() + 1 > SwiftConstants.MAX_META_COUNT)
		{
			throw new IllegalArgumentException("Too many metadata items; max " + SwiftConstants.MAX_META_COUNT);
		}
		if(!key.isEmpty())
		{
			metadata.put(key.toLowerCase(), value);
		}
	}

	public String getMetadataValue(String key)
	{
		return metadata.get(key.toLowerCase());
	}

	public void removeMetadataValue(String key)
	{
		metadata.remove(key);
	}

	public boolean hasMetadataKey(String key)
	{
		return metadata.containsKey(key.toLowerCase());
	}

	public void clearMetadata() { metadata.clear(); }

	public void setName(String resourceName)
	{
		this.name = resourceName;
	}

	public void setDeleteAt(Date deleteAt)
	{
		this.deleteAt = deleteAt;
	}

	@JsonProperty
	public int getBytes() {
		int sum = 0;
		for(SwiftResource child : getChildren().values())
		{
			sum += child.getBytes();
		}
		return sum;
	}

	@JsonIgnore
	public Map<String, String> getMetadata()
	{
		return metadata;
	}

	@JsonIgnore
	public Set<String> getMetadataKeys()
	{
		return metadata.keySet();
	}

	@JsonIgnore
	public NavigableMap<String, SwiftResource> getChildren()
	{
		return children;
	}

	@JsonIgnore
	public Long getId()
	{
		return id;
	}

	@JsonIgnore
	public String getName()
	{
		return name;
	}

	@JsonIgnore
	public Date getCreatedAt()
	{
		return createdAt;
	}

	@JsonIgnore
	public Date getDeleteAt() {
		return deleteAt;
	}

	@JsonIgnore
	public Date getLastModified()
	{
		return last_modified;
	}

	@JsonIgnore
	public int getCount() {
		return count;
	}
}
