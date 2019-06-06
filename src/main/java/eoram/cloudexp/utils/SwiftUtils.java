package eoram.cloudexp.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.ObjectMapper;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.command.shared.identity.access.KeystoneV3Access;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class SwiftUtils
{
	private static final SwiftUtils instance = new SwiftUtils();

	private SwiftUtils() {}

	public static SwiftUtils getInstance() { return instance; }

	private SwiftCredentials parseCredentials(File credentials)
	{
		SwiftCredentials ret = new SwiftCredentials();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(credentials));
			ret.username = br.readLine();
			ret.password = br.readLine();
			ret.authUrl = br.readLine();
			ret.authVersion = br.readLine();
			ret.domain = br.readLine();
			ret.region = br.readLine();
			ret.tenantName = br.readLine();
			ret.tenantId = br.readLine();
			br.close();
		}
		catch (IOException e) {}

		return ret;
	}

	public Account initialize(File credentials, String containerName, boolean resetContainer)
	{
		SwiftCredentials swiftCreds = parseCredentials(credentials);
		AccountConfig config = new AccountConfig();
		config.setUsername(swiftCreds.username);
		config.setPassword(swiftCreds.password);
		config.setAuthUrl(swiftCreds.authUrl);
		config.setDomain(swiftCreds.domain);
		config.setPreferredRegion(swiftCreds.region);
		config.setTenantName(swiftCreds.tenantName);
		config.setTenantId(swiftCreds.tenantId);
		AuthenticationMethod authMethod = null;

		if(swiftCreds.authVersion.equals("v1"))
		{
			authMethod = AuthenticationMethod.BASIC;
		}
		else if(swiftCreds.authVersion.equals("v2"))
		{
			authMethod = AuthenticationMethod.KEYSTONE;
		}
		else if(swiftCreds.authVersion.equals("v3"))
		{
			authMethod = AuthenticationMethod.KEYSTONE_V3;
		}
		else if(swiftCreds.authVersion.equals("v3scope"))
		{
			KeystoneV3AccessProviderWithScope ap = new KeystoneV3AccessProviderWithScope(config);
			config.setAccessProvider(ap);
			authMethod = AuthenticationMethod.EXTERNAL;
		}
		else
		{
			Errors.error("Unknown authentication version " + swiftCreds.authVersion);
		}

		return new AccountFactory(config).setAuthenticationMethod(authMethod).createAccount();
	}

	public void cloneContainer(Account account, Container srcContainer, String dstKey)
	{
		Container destContainer = account.getContainer(dstKey);

		Collection<StoredObject> srcObjects = srcContainer.list();
		for(StoredObject srcObject: srcObjects)
		{
			StoredObject destObject = destContainer.getObject(srcContainer.getName());
			srcObject.copyObject(destContainer, destObject);
		}
	}

	protected class SwiftCredentials
	{
		protected String username = null;
		protected String password = null;
		protected String authUrl = null;
		protected String authVersion = null;
		protected String domain = null;
		protected String region = null;
		protected String tenantName = null;
		protected String tenantId = null;
	}

	/**
	 * Adapted from https://github.com/Alluxio/alluxio/blob/master/underfs/swift/src/main/java/alluxio/underfs/swift/KeystoneV3AccessProvider.java
	 * Credit goes to The Alluxio Open Foundation
	 */
	public class KeystoneV3AccessProviderWithScope implements AuthenticationMethod.AccessProvider {
		private static final String AUTH_METHOD = "password";
		private static final int RESPONSE_OK = 201;

		private AccountConfig mAccountConfig;

		/**
		 * Create a new instance of {@link KeystoneV3AccessProviderWithScope}.
		 *
		 * @param accountConfig account credentials
		 */
		public KeystoneV3AccessProviderWithScope(AccountConfig accountConfig) {
			mAccountConfig = accountConfig;
		}

		@Override
		public Access authenticate() {
			try {
				String requestBody;
				try {
					// Construct request body
					KeystoneV3Request request =
							new KeystoneV3Request(new Auth(
									new Identity(Arrays.asList(AUTH_METHOD),
											new Password(
													new User(mAccountConfig.getUsername(), mAccountConfig.getPassword(), new Domain(mAccountConfig.getDomain())))),
									new Scope(new Project(mAccountConfig.getTenantId(), mAccountConfig.getTenantName(), new Domain(mAccountConfig.getDomain())))));
					requestBody = new ObjectMapper().writeValueAsString(request);
				} catch (JsonProcessingException e) {
					Errors.error("Error processing JSON request: ");
					return null;
				}

				try (CloseableHttpClient client = HttpClients.createDefault()) {
					// Send request
					HttpPost post = new HttpPost(mAccountConfig.getAuthUrl());
					post.addHeader("Accept", "application/json");
					post.addHeader("Content-Type", "application/json");
					post.setEntity(new ByteArrayEntity(requestBody.toString().getBytes()));
					try (CloseableHttpResponse httpResponse = client.execute(post)) {
						// Parse response
						int responseCode = httpResponse.getStatusLine().getStatusCode();
						if (responseCode != RESPONSE_OK) {
							Errors.error("Error with response code " + responseCode);
							return null;
						}
						String token = httpResponse.getFirstHeader("X-Subject-Token").getValue();

						// Parse response body
						try (BufferedReader bufReader =
									 new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()))) {
							String responseBody = bufReader.readLine();
							KeystoneV3Response response;
							try {
								JsonNode node = new ObjectMapper().readTree(responseBody);
								// Construct access object
								KeystoneV3Access access = new KeystoneV3Access(token, node);
								return access;
							} catch (JsonProcessingException e) {
								Errors.error("Error processing JSON response: " + e.getMessage());
								return null;
							}
						}
					}
				}
			} catch (IOException e) {
				// Unable to authenticate
				Errors.error("Exception authenticating using KeystoneV3 " + e.getMessage());
				return null;
			}
		}

		/** Classes for creating authentication JSON request. */
		@JsonPropertyOrder({"auth"})
		private class KeystoneV3Request {
			@JsonProperty("auth")
			public Auth mAuth;

			public KeystoneV3Request(Auth auth) {
				mAuth = auth;
			}
		}

		@JsonPropertyOrder({"identity", "scope"})
		private class Auth {
			@JsonProperty("identity")
			public Identity mIdentity;
			@JsonProperty("scope")
			public Scope mScope;

			public Auth(Identity identity, Scope scope) {
				mIdentity = identity;
				mScope = scope;
			}
		}

		@JsonPropertyOrder({"methods", "password"})
		private class Identity {
			@JsonProperty("methods")
			public List<String> mMethods = null;
			@JsonProperty("password")
			public Password mPassword;

			public Identity(List<String> methods, Password password) {
				mMethods = methods;
				mPassword = password;
			}
		}

		@JsonPropertyOrder({"user"})
		private class Password {
			@JsonProperty("user")
			public User mUser;

			public Password(User user) {
				mUser = user;
			}
		}

		@JsonPropertyOrder({"name"})
		private class Domain {
			@JsonProperty("name")
			public String mName;

			public Domain(String name) {
				mName = name;
			}
		}

		@JsonPropertyOrder({"id", "name", "domain"})
		private class Project {
			@JsonProperty("id")
			public String mId;
			@JsonProperty("name")
			public String mName;
			@JsonProperty("domain")
			public Domain mDomain;

			public Project(String id, String name, Domain domain) {
				mId = id;
				mName = name;
				mDomain = domain;
			}
		}

		@JsonPropertyOrder({"project"})
		private class Scope {
			@JsonProperty("project")
			public Project mProject;

			public Scope(Project project) {
				mProject = project;
			}
		}

		@JsonPropertyOrder({"name", "password", "domain"})
		private class User {
			@JsonProperty("name")
			public String mName;
			@JsonProperty("password")
			public String mPassword;
			@JsonProperty("domain")
			public Domain mDomain;

			public User(String name, String password, Domain domain) {
				mName = name;
				mPassword = password;
				mDomain = domain;
			}
		}

		/** Classes for parsing authentication JSON response. */
		@JsonIgnoreProperties(ignoreUnknown = true)
		private class KeystoneV3Response {
			public Token mToken;

			@JsonCreator
			public KeystoneV3Response(@JsonProperty("token") Token token) {
				mToken = token;
			}
		}

		@JsonIgnoreProperties(ignoreUnknown = true)
		private class Token {
			public List<Catalog> mCatalog = null;

			@JsonCreator
			public Token(@JsonProperty("catalog") List<Catalog> catalog) {
				mCatalog = catalog;
			}
		}

		@JsonIgnoreProperties(ignoreUnknown = true)
		private class Catalog {
			public List<Endpoint> mEndpoints;
			public String mType;
			public String mName;

			@JsonCreator
			public Catalog(@JsonProperty("endpoints") List<Endpoint> endpoints,
						   @JsonProperty("type") String type, @JsonProperty("name") String name) {
				mEndpoints = endpoints;
				mType = type;
				mName = name;
			}
		}

		@JsonIgnoreProperties(ignoreUnknown = true)
		private class Endpoint {
			public String mUrl;
			public String mRegion;
			public String mInterface;

			@JsonCreator
			public Endpoint(@JsonProperty("url") String url, @JsonProperty("region") String region,
							@JsonProperty("interface") String inter) {
				mUrl = url;
				mRegion = region;
				mInterface = inter;
			}
		}
	}
}
