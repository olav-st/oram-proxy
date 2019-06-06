package eoram.cloudexp.service.application.http.swiftapi.auth.v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import eoram.cloudexp.utils.MiscUtils;
import org.joda.time.LocalDate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@JsonRootName(value = "access")
public class SwiftAccess
{
	@JsonProperty
	public final SwiftToken token;
	public List<SwiftService> serviceCatalog = new ArrayList<>();
	public final SwiftUser user = new SwiftUser();
	public final SwiftMetadata metadata = new SwiftMetadata();

	public SwiftAccess(String tokenStr, String storageUrl, String authUrl)
	{
		token = new SwiftToken(tokenStr);
		serviceCatalog.add(new SwiftService("object-store", "swift", Arrays.asList(storageUrl)));
		serviceCatalog.add(new SwiftService("identity", "keystone",  Arrays.asList(authUrl)));
	}

	public class SwiftToken
	{
		@JsonProperty
		public String id = "v2.0";
		public final Date issued_at = new Date();
		public final Date expires = LocalDate.fromDateFields(new Date()).plusMonths(1).toDate();
		public final SwiftTenant tenant = new SwiftTenant();

		public SwiftToken(String tokenStr)
		{
			id = tokenStr;
		}

		public class SwiftTenant
		{
			@JsonProperty
			public String id = "v2.0";
			public String name = "test_tenant";
			public String description = "test";
			public boolean enabled = true;
		}
	}

	public class SwiftService
	{
		@JsonProperty
		public final String type;
		public final String name;
		public final List<String> endpoints_links = new ArrayList<>();
		public final List<SwiftEndpoint> endpoints = new ArrayList<>();

		public SwiftService(String type, String name, List<String> urls)
		{
			this.type = type;
			this.name = name;
			for(String url : urls)
			{
				String id = MiscUtils.getInstance().getRandomHexString(32);
				endpoints.add(new SwiftEndpoint(id, "RegionOne", url));
			}
		}

		public class SwiftEndpoint
		{
			@JsonProperty
			public String id;
			public String region;
			public String publicURL;
			public String internalURL;
			public String adminURL;

			public SwiftEndpoint(String id, String region, String storageUrl)
			{
				this.id = id;
				this.region = region;
				this.publicURL = storageUrl;
				this.internalURL = storageUrl;
				this.adminURL = storageUrl;
			}
		}
	}

	public class SwiftUser
	{
		@JsonProperty
		public final String id = "test";
		public final String username = "test";
		public final String name = "test";
		public final List<String> roles_link = new ArrayList<>();
		public final List<SwiftRole> roles = Arrays.asList(new SwiftRole("service"));

		public class SwiftRole
		{
			public String name;

			public SwiftRole(String name)
			{
				this.name = name;
			}
		}
	}

	public class SwiftMetadata
	{
		@JsonProperty
		public final int is_admin = 0;
		public final List<String> roles = new ArrayList<>();
	}
}
