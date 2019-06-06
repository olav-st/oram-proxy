package eoram.cloudexp.service.application.http.swiftapi.auth.v2;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;

public class SwiftTenantList
{
	@JsonProperty
	public final List<SwiftTenant> tenants = Arrays.asList(new SwiftTenant());

	public class SwiftTenant
	{
		@JsonProperty
		public String id = "test";
		public String name = "test";
		public String description = "sample description";
		public boolean enabled = true;
	}
}
