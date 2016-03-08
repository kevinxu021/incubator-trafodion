package com.esgyn.dbmgr.common;

import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RESTRequestException extends EsgynDBMgrException {
	private static final long serialVersionUID = 1L;
	private String uri;

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public RESTRequestException() {
		super("Unexpected exception");
	}

	public RESTRequestException(String message) {
		super(message);
	}

	public RESTRequestException(Status status, String message) {
		super(status, message);
	}

	public RESTRequestException(String message, Throwable cause) {
		super(message, cause);
	}

	public RESTRequestException(Status status, String message, Throwable cause) {
		super(status, message, cause);
	}

	public RESTRequestException(String uri, Status status, String message) {
		super(status, message);
		setUri(uri);
	}

	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode objNode = mapper.createObjectNode();
		objNode.put("url", getUri());
		objNode.put("httpCode", getStatus().getStatusCode());
		objNode.put("message", getMessage());
		try {
			return mapper.writeValueAsString(objNode);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
}
