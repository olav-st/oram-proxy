package eoram.cloudexp.service.application.http.swiftapi.resources;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import eoram.cloudexp.service.application.http.swiftapi.SwiftConstants;

import java.util.ArrayList;
import java.util.List;

@JsonRootName(value = "container")
public class SwiftContainer extends SwiftResource
{
	private String storagePolicy = "default";

	public SwiftContainer(String name)
	{
		super(name);
		if(name.length() > SwiftConstants.MAX_CONTAINER_NAME_LENGTH)
		{
			throw new IllegalArgumentException("Container name length of " + name.length() + " longer than " + SwiftConstants.MAX_CONTAINER_NAME_LENGTH);
		}
	}

	public SwiftContainer(SwiftContainer other)
	{
		super(other);
		this.storagePolicy = other.getStoragePolicy();
	}

	public String getStoragePolicy() {
		return storagePolicy;
	}

	public void setStoragePolicy(String storagePolicy) {
		this.storagePolicy = storagePolicy;
	}

	@JacksonXmlElementWrapper(useWrapping=false)
	@JacksonXmlProperty(localName = "object")
	public List<SwiftResource> getObjects()
	{
		return new ArrayList<>(getChildren().values());
	}

	@JacksonXmlProperty(isAttribute = true, localName =" name")
	public String getNameAsAttribute()
	{
		return name;
	}
}
