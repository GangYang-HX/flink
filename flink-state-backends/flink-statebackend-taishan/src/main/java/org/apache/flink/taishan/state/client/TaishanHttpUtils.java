package org.apache.flink.taishan.state.client;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.util.concurrent.TimeoutException;

/**
 * @author Dove
 * @Date 2022/6/23 5:24 下午
 */
public class TaishanHttpUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanHttpUtils.class);
	private static final String HTTP_ERROR_MESSAGE = "Request Taishan Error.";
	private static final String HTTP_ERROR_STATE = "HttpError";

	public static ResponseBody createTaishanTable(TaishanAdminConfiguration configuration, String tableName, int maxParallelism) {
		try (CloseableHttpClient httpclient = HttpClients.createDefault();) {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode nodes = mapper.createObjectNode();
			nodes.put("name", tableName);
			nodes.put("cluster", configuration.getCluster());
			nodes.put("shard_count", maxParallelism);
			nodes.put("group_token", configuration.getGroupToken());
			nodes.put("group_name", configuration.getGroupName());
			ObjectNode propertiesNode = mapper.createObjectNode();
			propertiesNode.put("saberJobId", configuration.getSaberJobId());
			nodes.set("properties", propertiesNode);
			ArrayNode apps = mapper.createArrayNode();
			apps.add(configuration.getAppName());
			nodes.set("apps", apps);

			HttpPost httpPost = new HttpPost(configuration.getCreateDropUrl());
			httpPost.addHeader("Content-Type", "application/json;charset=UTF-8");

			StringEntity stringEntity = new StringEntity(mapper.writeValueAsString(nodes), "UTF-8");
			LOG.info("Create taishan table http request body: {}", mapper.writeValueAsString(nodes));
			stringEntity.setContentEncoding("UTF-8");

			httpPost.setEntity(stringEntity);

			// Create a custom response handler
			ResponseHandler<String> responseHandler = response -> {
				int status = response.getStatusLine().getStatusCode();
				if (status == 200) {
					HttpEntity entity = response.getEntity();
					return entity != null ? EntityUtils.toString(entity) : null;
				} else {
					throw new ClientProtocolException(
						"Unexpected response status: " + status);
				}
			};
			String responseContext = httpclient.execute(httpPost, responseHandler);

			return parseResponse(mapper, responseContext, "access_token");
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while create taishan table.", e);
		}
	}

	public static ResponseBody queryTaishanTable(TaishanAdminConfiguration configuration, String tableName, long timeOut) throws TimeoutException {
		long start = System.currentTimeMillis();
		ResponseBody responseBody = queryTaishanTable(configuration, tableName);
		while (!responseBody.isNormal() && (System.currentTimeMillis() - start < timeOut)) {
			try {
				LOG.warn("Wait for Taishan table({}) create finished.", tableName);
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
			responseBody = queryTaishanTable(configuration, tableName);
		}
		if (responseBody.isNormal()) {
			return responseBody;
		}
		throw new TimeoutException("Query Taishan Table is not normal, table is " + tableName + ", timeout(ms):" + timeOut);
	}

	public static ResponseBody queryTaishanTable(TaishanAdminConfiguration configuration, String tableName) {
		try (CloseableHttpClient httpclient = HttpClients.createDefault();) {
			StringBuffer params = new StringBuffer();
			params.append("name=").append(URLEncoder.encode(tableName, "utf-8"));
			params.append("&cluster=").append(configuration.getCluster());
			params.append("&group_name=").append(configuration.getGroupName());
			params.append("&group_token=").append(configuration.getGroupToken());

			HttpGet httpGet = new HttpGet(configuration.getQueryUrl() + "?" + params.toString());
			httpGet.addHeader("Content-Type", "application/x-www-form-urlencoded");

			// Create a custom response handler
			ResponseHandler<String> responseHandler = response -> {
				int status = response.getStatusLine().getStatusCode();
				if (status == 200) {
					HttpEntity entity = response.getEntity();
					return entity != null ? EntityUtils.toString(entity) : null;
				} else {
					LOG.warn("Unexpected response status:" + status);
					return null;
				}
			};
			String responseContext = httpclient.execute(httpGet, responseHandler);

			return parseResponse(new ObjectMapper(), responseContext, "token");
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while query taishan table.", e);
		}
	}

	public static ResponseBody dropTaishanTable(TaishanAdminConfiguration configuration, String tableName, String accessToken) {
		try (CloseableHttpClient httpclient = HttpClients.createDefault();) {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode nodes = mapper.createObjectNode();
			nodes.put("name", tableName);
			nodes.put("cluster", configuration.getCluster());
			nodes.put("group_token", configuration.getGroupToken());
			nodes.put("group_name", configuration.getGroupName());
			nodes.put("access_token", accessToken);

			HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(configuration.getCreateDropUrl());
			httpDelete.addHeader("Content-Type", "application/json;charset=UTF-8");

			StringEntity stringEntity = new StringEntity(mapper.writeValueAsString(nodes), "UTF-8");
			stringEntity.setContentEncoding("UTF-8");
			httpDelete.setEntity(stringEntity);

			// Create a custom response handler
			ResponseHandler<String> responseHandler = response -> {
				int status = response.getStatusLine().getStatusCode();
				if (status == 200) {
					HttpEntity entity = response.getEntity();
					return entity != null ? EntityUtils.toString(entity) : null;
				} else {
					LOG.warn("Unexpected response status:" + status);
					return null;
				}
			};
			String responseContext = httpclient.execute(httpDelete, responseHandler);

			return parseResponse(mapper, responseContext, "access_token");
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while drop taishan table.", e);
		}
	}

	private static ResponseBody parseResponse(ObjectMapper mapper, String responseContext, String token) throws JsonProcessingException {
		if (responseContext == null) {
			return new ResponseBody(false, -1, HTTP_ERROR_MESSAGE, "", HTTP_ERROR_STATE);
		}
		JsonNode jsonNode = mapper.readTree(responseContext);
		JsonNode codeNode = jsonNode.get("code");
		JsonNode successNode = jsonNode.get("success");
		JsonNode messageNode = jsonNode.get("message");
		JsonNode dataNode = jsonNode.get("data");
		JsonNode stateNode = dataNode.get("state");
		JsonNode accessTokenNode = dataNode.get(token);
		String accessToken = "";
		if (accessTokenNode != null) {
			accessToken = accessTokenNode.asText();
			accessToken = accessToken.split(",")[0];
		}
		String state = "";
		if (stateNode != null) {
			state = stateNode.asText();
		}
		return new ResponseBody(successNode.asBoolean(), codeNode.asInt(), messageNode.asText(), accessToken, state);
	}

	public static class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
		public static final String METHOD_NAME = "DELETE";

		public String getMethod() {
			return METHOD_NAME;
		}

		public HttpDeleteWithBody(final String uri) {
			super();
			setURI(URI.create(uri));
		}

		public HttpDeleteWithBody(final URI uri) {
			super();
			setURI(uri);
		}

		public HttpDeleteWithBody() {
			super();
		}
	}

	public static class ResponseBody {
		private boolean success;
		private int code;
		private String message;
		private String accessToken;
		private String state;

		public ResponseBody(boolean success, int code, String message, String accessToken, String state) {
			this.success = success;
			this.code = code;
			this.message = message;
			this.accessToken = accessToken;
			this.state = state;
		}

		public boolean isSuccess() {
			return success;
		}

		public int getCode() {
			return code;
		}

		public String getMessage() {
			return message;
		}

		public String getAccessToken() {
			return accessToken;
		}

		public String getState() {
			return state;
		}

		public boolean isNormal() {
			return "Normal".equals(state);
		}

		@Override
		public String toString() {
			return "ResponseBody{" +
				"success=" + success +
				", code=" + code +
				", message='" + message + '\'' +
				", accessToken='" + accessToken + '\'' +
				", state='" + state + '\'' +
				'}';
		}
	}
}
