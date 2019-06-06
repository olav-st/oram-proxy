package eoram.cloudexp.service.application.http.swiftapi.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import eoram.cloudexp.service.application.http.swiftapi.resources.SwiftResource;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.stream.Collectors;

public class SerializationUtils
{
	public static String serializeSwiftResource(SwiftResource resource, String format)
	{
		String result = "";
		if(format.equals("text"))
		{
			result = resource.getChildren().values().stream().map(SwiftResource::getName).collect(Collectors.joining("\n"));
		}
		else
		{
			try {
				ObjectMapper mapper = getObjectMapper(format);
				SimpleModule testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
				testModule.addSerializer(SwiftResource.class, new SwiftResourceSerializer());
				//mapper.registerModule(testModule);
				if(format.equals("json"))
				{
					result = mapper.writeValueAsString(resource.getChildren().values());
				}else
				{
					result = mapper.writeValueAsString(resource);

				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public static ObjectMapper getObjectMapper(String format)
	{
		ObjectMapper mapper = null;
		if(format.equals("xml"))
		{
			mapper = new XmlMapper();
		}
		else
		{
			mapper = new ObjectMapper();
		}
		mapper.setDateFormat(new SimpleDateFormat("Y-MM-dd'T'HH:mm:ss.S"));
		return mapper;
	}

	public static class SwiftResourceSerializer extends StdSerializer<SwiftResource> {

		public SwiftResourceSerializer() {
			this(null);
		}

		public SwiftResourceSerializer(Class<SwiftResource> t) {
			super(t);
		}

		@Override
		public void serialize(SwiftResource value, JsonGenerator jgen, SerializerProvider provider)
				throws IOException {
			jgen.writeObject(value.getChildren());
		}
	}
}
