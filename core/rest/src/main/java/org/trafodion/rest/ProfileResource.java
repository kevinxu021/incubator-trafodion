/**
* @@@ START COPYRIGHT @@@                                                       
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
* @@@ END COPYRIGHT @@@
*/

package org.trafodion.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;


public class ProfileResource extends ResourceBase {
	private static final Log LOG =
		LogFactory.getLog(ProfileResource.class);

	static CacheControl cacheControl;
	static {
		cacheControl = new CacheControl();
		cacheControl.setNoCache(true);
		cacheControl.setNoTransform(false);
	}

	public ProfileResource() throws IOException {
		super();
	}
	
	private String buildRemoteException(String className,String exception,String message) throws IOException {
  
	    try {
	        JSONObject jsonRemoteExceptionDetail = new JSONObject();
	        jsonRemoteExceptionDetail.put("javaClassName", className);
	        jsonRemoteExceptionDetail.put("exception",exception);
	        jsonRemoteExceptionDetail.put("message",message);
	        JSONObject jsonRemoteException  = new JSONObject();	        
	        jsonRemoteException.put("RemoteException",jsonRemoteExceptionDetail);
	        if (LOG.isDebugEnabled()) 
	            LOG.debug(jsonRemoteException.toString());
	        return jsonRemoteException.toString();
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw new IOException(e);
	    }
	}
    private JSONObject profile() throws IOException {
        JSONObject json = null;
        try {
            Map<String, LinkedHashMap<String,String>> profiles = servlet.getWmsProfilesMap();
            json= new JSONObject(profiles);
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        if(LOG.isDebugEnabled())
            LOG.debug("json.length() = " + json.length());

        return json;
    }    
   
    @GET
    @Path("/test")
    @Produces({MIMETYPE_JSON})
    public Response test(
            final @Context UriInfo uriInfo,
            final @Context Request request) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("GET " + uriInfo.getAbsolutePath());

                MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
                String output = " Query Parameters :\n";
                for (String key : queryParams.keySet()) {
                    output += key + " : " + queryParams.getFirst(key) +"\n";
                }
                LOG.debug(output);

                MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
                output = " Path Parameters :\n";
                for (String key : pathParams.keySet()) {
                    output += key + " : " + pathParams.getFirst(key) +"\n";
                }
                LOG.debug(output);
            }
 
            String result = buildRemoteException(
                    "org.trafodion.rest.NotFoundException",
                    "NotFoundException",
                    "This is my exception text");
            return Response.status(Response.Status.NOT_FOUND)
                    .type(MIMETYPE_JSON).entity(result + CRLF)
                    .build();
            
            //ResponseBuilder response = Response.ok(result);
            //response.cacheControl(cacheControl);
            //return response.build();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    }    
    
    @GET
    @Produces({MIMETYPE_JSON})
    public Response getAll(
            final @Context UriInfo uriInfo,
            final @Context Request request) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("GET " + uriInfo.getAbsolutePath());

                MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
                String output = " Query Parameters :\n";
                for (String key : queryParams.keySet()) {
                    output += key + " : " + queryParams.getFirst(key) +"\n";
                }
                LOG.debug(output);

                MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
                output = " Path Parameters :\n";
                for (String key : pathParams.keySet()) {
                    output += key + " : " + pathParams.getFirst(key) +"\n";
                }
                LOG.debug(output);
            }

            JSONObject json = profile();
            
            if(json.length() == 0) {
                String result = buildRemoteException(
                        "org.trafodion.rest.NotFoundException",
                        "NotFoundException",
                        "No server resources found");
                return Response.status(Response.Status.NOT_FOUND)
                        .type(MIMETYPE_JSON).entity(result + CRLF)
                        .build();
            }
 
            ResponseBuilder response = Response.ok(json.toString());
            response.cacheControl(cacheControl);
            return response.build();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    } 
    
    @POST
    @Consumes({MIMETYPE_JSON})
    public Response postProfile(String data) {
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("POST " + data);
            String sdata = "";
            Response.Status status = Response.Status.OK; 
            String result = "200 OK.";

            JSONTokener jsonParser = new JSONTokener(data);
            JSONObject json = (JSONObject)jsonParser.nextValue();

            Iterator<?> keysItr = json.keys();
            while(keysItr.hasNext()) {
                String key = (String)keysItr.next();
                JSONObject value = (JSONObject)json.get(key);
                Iterator<?> itr = value.keys();
                sdata = "";
                boolean lastUpdate = false;

                while(itr.hasNext()) {
                    String attrKey = (String)itr.next();
                    String attrValue = value.getString(attrKey);
                    if (sdata.length()!= 0)
                        sdata = sdata + ":";
                    if(attrKey.equals(Constants.LAST_UPDATE)){
                        lastUpdate = true;
                        attrValue = Long.toString(System.currentTimeMillis());
                    }
                    sdata = sdata + attrKey + "=" + attrValue;
                 } 
                if (lastUpdate == false)
                    sdata = sdata + ";" + Constants.LAST_UPDATE + "=" + Long.toString(System.currentTimeMillis());
               status = servlet.postWmsProfile(key, sdata);
            }
            if (status == Response.Status.CREATED)
                result = "201 Created.";
            return Response.status(status).type(MIMETYPE_TEXT).entity(result + CRLF).build();
        } catch (Exception e) {
            e.printStackTrace();

            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    }    
    @DELETE 
    @Path("/query")
    public Response deleteProfile(@QueryParam("delete") String name) {
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("DELETE name :" + name);
            Response.Status status = Response.Status.OK; 
            String result = "200 OK.";
            servlet.deleteWmsProfile(name);
            
            return Response.status(status).type(MIMETYPE_TEXT).entity(result + CRLF).build();
            
        } catch (Exception e) {
            e.printStackTrace();

            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    }
    
}