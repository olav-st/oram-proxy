package eoram.cloudexp.service.application.http.swiftapi.auth.v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@JsonRootName(value = "version")
public class SwiftVersion
{
	@JsonProperty
	public final String id = "v2.0";
	public final String status = "stable";
	public final Date updated = new Date();
	@JsonProperty("media-types")
	public List<SwiftMediaType> mediaTypes = new ArrayList<>();
	@JsonProperty
	public List<SwiftLink> links = new ArrayList<>();

	public SwiftVersion()
	{
		mediaTypes.add(new SwiftMediaType(
				"application/json",
				"application/vnd.openstack.identity-v2.0+json"
		));
		links.add(new SwiftLink(
				"describedby",
				"http://docs.openstack.org/",
				"text/html"
		));
	}

	public void addLink(String rel, String href)
	{
		links.add(new SwiftLink(rel, href));
	}

	public final class SwiftMediaType
	{
		@JsonProperty
		public String base;
		public String type;

		public SwiftMediaType(String b, String t)
		{
			base = b;
			type = t;
		}
	}

	public final class SwiftLink
	{
		@JsonProperty
		public String href;
		public String rel;
		public String type;

		public SwiftLink(String r, String h)
		{
			href = h;
			rel = r;
		}

		public SwiftLink(String r, String h, String t)
		{
			href = h;
			rel = r;
			type = t;
		}
	}
}
