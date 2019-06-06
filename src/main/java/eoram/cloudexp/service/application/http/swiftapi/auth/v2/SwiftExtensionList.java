package eoram.cloudexp.service.application.http.swiftapi.auth.v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@JsonRootName(value = "extensions")
public class SwiftExtensionList {
	@JsonProperty
	public List<SwiftExtension> values = Arrays.asList(new SwiftExtension());

	public class SwiftExtension
	{
		@JsonProperty
		public final String name = "sample_extension";
		public final Date updated = new Date();
		public final String alias = "sample";
		public final String namespace = "sample_namespace";
		public final String description = "sample_description";
		public final List<String> links = new ArrayList<>();
	}
}
