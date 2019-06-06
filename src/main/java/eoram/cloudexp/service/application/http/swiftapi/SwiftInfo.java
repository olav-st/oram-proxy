package eoram.cloudexp.service.application.http.swiftapi;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a response to an /info request in the OpenStack Swift API.
 * Based on: https://github.com/bouncestorage/swiftproxy/blob/master/src/main/java/com/bouncestorage/swiftproxy/v1/InfoResource.java
 *
 */
public final class SwiftInfo
{
	@JsonProperty
	public final SwiftConfiguration swift = new SwiftConfiguration();

	static final class SwiftConfiguration
	{
		@JsonProperty
		public final int account_listing_limit = SwiftConstants.ACCOUNT_LISTING_LIMIT;
		public final int container_listing_limit = SwiftConstants.CONTAINER_LISTING_LIMIT;
		public final int max_account_name_length = SwiftConstants.MAX_ACCOUNT_NAME_LENGTH;
		public final int max_container_name_length = SwiftConstants.MAX_CONTAINER_NAME_LENGTH;
		public final long max_file_size = SwiftConstants.MAX_FILE_SIZE;
		public final int max_header_size = SwiftConstants.MAX_HEADER_SIZE;
		public final int max_meta_name_length = SwiftConstants.MAX_META_NAME_LENGTH;
		public final int max_meta_value_length = SwiftConstants.MAX_META_VALUE_LENGTH;
		public final int max_meta_count = SwiftConstants.MAX_META_COUNT;
		public final int max_meta_overall_size = SwiftConstants.MAX_META_OVERALL_SIZE;
		public final int max_object_name_length = SwiftConstants.MAX_OBJECT_NAME_LENGTH;
		public final boolean allow_account_management = false;
		public final boolean strict_cors_mode = true;
	}
}
