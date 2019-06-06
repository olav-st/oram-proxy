package eoram.cloudexp.service.application.http.swiftapi.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SwiftDirectory extends SwiftResource
{
	@JsonProperty
	protected String subdir = "";

	public SwiftDirectory(String dir)
	{
		super(dir);
		subdir = dir;
	}
}
