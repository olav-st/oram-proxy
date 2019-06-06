package eoram.cloudexp.service.application.http.swiftapi.resources;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import eoram.cloudexp.service.application.http.swiftapi.SwiftConstants;
import eoram.cloudexp.utils.EncodingUtils;
import eoram.cloudexp.utils.MiscUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@JsonRootName(value = "object")
public class SwiftObject extends SwiftResource
{
	private int bytes = 0;
	@JsonProperty
	protected String hash = "";
	@JsonProperty
	protected String content_type = "";

	public SwiftObject(String name, byte[] data, String contentType)
	{
		super(name);
		if(name.length() > SwiftConstants.MAX_OBJECT_NAME_LENGTH)
		{
			throw new IllegalArgumentException("Object name length of " + name.length() + " longer than " + SwiftConstants.MAX_OBJECT_NAME_LENGTH);
		}
		hash = MiscUtils.getInstance().getMD5Hash(data);
		bytes = data.length;
		setContentType(contentType);
	}

	public SwiftObject(String name, byte[] data)
	{
		this(name, data, "application/octet-stream");
	}

	public SwiftObject(SwiftObject other)
	{
		super(other);
		this.bytes = other.getBytes();
		this.hash = other.getHash();
		this.content_type = other.getContentType();
	}

	public void setContentType(String contentType)
	{
		this.content_type = contentType;
		setMetadataValue("content-type", contentType);
	}

	@JsonProperty
	@Override
	public int getBytes()
	{
		return bytes;
	}

	@JsonIgnore
	public String getContentType()
	{
		return content_type;
	}

	@JsonIgnore
	public String getHash()
	{
		return hash;
	}
}
