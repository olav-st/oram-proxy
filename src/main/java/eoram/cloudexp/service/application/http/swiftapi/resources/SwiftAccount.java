package eoram.cloudexp.service.application.http.swiftapi.resources;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import eoram.cloudexp.service.application.http.swiftapi.SwiftConstants;

import java.util.ArrayList;
import java.util.List;

@JsonRootName(value = "account")
public class SwiftAccount extends SwiftResource
{
	public SwiftAccount(String name)
	{
		super(name);
		if(name.length() > SwiftConstants.MAX_ACCOUNT_NAME_LENGTH)
		{
			throw new IllegalArgumentException("Account name length of " + name.length() + " longer than " + SwiftConstants.MAX_ACCOUNT_NAME_LENGTH);
		}
	}

	public SwiftAccount(SwiftAccount other)
	{
		super(other);
	}

	@JacksonXmlElementWrapper(useWrapping=false)
	@JacksonXmlProperty(localName = "container")
	public List<SwiftResource> getContainers()
	{
		return new ArrayList<>(getChildren().values());
	}

	@JacksonXmlProperty(isAttribute = true, localName =" name")
	public String getNameAsAttribute()
	{
		return name;
	}
}
