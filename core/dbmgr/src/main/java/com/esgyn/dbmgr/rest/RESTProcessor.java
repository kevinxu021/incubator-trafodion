package com.esgyn.dbmgr.rest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class RESTProcessor {

  private static final Logger _LOG = LoggerFactory.getLogger(RESTProcessor.class);

  private static TrustManager[] get_trust_mgr() {
    TrustManager[] certs = new TrustManager[] { new X509TrustManager() {
      public X509Certificate[] getAcceptedIssuers() {
        return null;
      }

      @Override
      public void checkClientTrusted(X509Certificate[] arg0, String arg1)
          throws CertificateException {
        // TODO Auto-generated method stub

      }

      @Override
      public void checkServerTrusted(X509Certificate[] arg0, String arg1)
          throws CertificateException {
        // TODO Auto-generated method stub

      }
    } };
    return certs;
  }

  public static String getRestOutput(String jsonString, String userName, String password)
      throws Exception {
    StringBuffer outputBuffer = new StringBuffer();
    HttpURLConnection conn = null;

    try {
      SSLContext ssl_ctx = SSLContext.getInstance("TLS");
      TrustManager[] trust_mgr = get_trust_mgr();
      ssl_ctx.init(null, // key manager
        trust_mgr, // trust manager
        new SecureRandom()); // random number generator
      HttpsURLConnection.setDefaultSSLSocketFactory(ssl_ctx.getSocketFactory());

      JsonFactory factory = new JsonFactory();
      ObjectMapper mapper = new ObjectMapper(factory);
      RESTRequest a = mapper.readValue(jsonString, RESTRequest.class);

      String uri = a.url;

      String parameterString = "";
      if (a.parameters != null && a.parameters.size() > 0) {
        Object[] paramKeys = a.parameters.keySet().toArray();
        for (int i = 0; i < paramKeys.length; i++) {
          String value = a.parameters.get(paramKeys[i]);
          parameterString += URLEncoder.encode(paramKeys[i].toString(), "UTF8") + "="
              + URLEncoder.encode(value, "UTF8");
          if (i < paramKeys.length - 1) parameterString += "&";
        }
      }

      if (a.method != null
          && (a.method.equalsIgnoreCase("get") || a.method.equalsIgnoreCase("delete"))) {
        if (parameterString.length() > 0) {
          uri = uri + "?" + parameterString;
        }
      }

      URL url = new URL(uri);

      _LOG.debug("Initiate REST call : " + jsonString);

      if (uri.indexOf("https") >= 0) {
        conn = (HttpsURLConnection) url.openConnection();
        // Guard against "bad host name" errors during handshake.
        ((HttpsURLConnection) conn).setHostnameVerifier(new HostnameVerifier() {
          @Override
          public boolean verify(String arg0, SSLSession arg1) {
            return true; // temporarily disable this check..
          }
        });
      } else conn = (HttpURLConnection) url.openConnection();

      conn.setRequestProperty("Accept", "application/json, application/text, text/plain");
      if (a.authorization == null || a.authorization.length() == 0) {
        a.authorization = userName + ":" + password;
      }

      if (a.authorization != null) {
        String[] userNamePassword = a.authorization.split(":");
        conn.setRequestProperty("user", userNamePassword[0]);
        String basicAuth = "Basic "
            + javax.xml.bind.DatatypeConverter.printBase64Binary(a.authorization.getBytes());
        conn.setRequestProperty("Authorization", basicAuth);
      }

      if (a.method != null && a.method.equalsIgnoreCase("delete")) {
        conn.setDoOutput(true);
        conn.setRequestMethod(a.method.toUpperCase());
      }

      if (a.method != null
          && (a.method.equalsIgnoreCase("post") || a.method.equalsIgnoreCase("put"))) {
        conn.setDoOutput(true);
        conn.setRequestMethod(a.method.toUpperCase());
        OutputStream os = conn.getOutputStream();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        writer.write(parameterString);
        writer.close();
        os.close();
      }

      if (conn.getResponseCode() != 200) {
        String message = conn.getResponseMessage();

        InputStream es = conn.getErrorStream();
        if (es != null) {
          BufferedReader br = new BufferedReader(new InputStreamReader(es));

          String output = "";
          while ((output = br.readLine()) != null) {
            outputBuffer.append(output);
            outputBuffer.append(System.getProperty("line.separator"));
          }
          message = outputBuffer.toString();
        }

        throw new RuntimeException(
            a.url + ", Http Error: " + conn.getResponseCode() + ", " + message);
      }

      BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

      String output = "";
      while ((output = br.readLine()) != null) {
        outputBuffer.append(output);
        outputBuffer.append(System.getProperty("line.separator"));
      }

    } catch (Exception ex) {
      _LOG.debug("Error processing the REST request : " + ex.toString());
      throw new Exception("Error processing the REST request " + ex.getMessage());
    } finally {

      if (conn != null) {
        conn.disconnect();
      }
    }

    return outputBuffer.toString();
  }

  public static JsonNode findArrayNode(JsonNode parentNode) {
    if (parentNode instanceof ObjectNode) {
      Iterator<Map.Entry<String, JsonNode>> childNodes = parentNode.fields();

      while (childNodes.hasNext()) {
        Map.Entry<String, JsonNode> childNode = childNodes.next();
        if (childNode.getValue().isArray()) {
          return childNode.getValue();
        } else {
          JsonNode arrayNode = findArrayNode(childNode.getValue());
          if (arrayNode != null) return arrayNode;
        }
      }
    }
    return null;
  }

  public static void processResult(RESTRequest restRequest, String jsonString,
      ArrayList<String> columns, ArrayList<Object[]> rows)
          throws JsonParseException, JsonMappingException, IOException {
    processResult(restRequest, jsonString, columns, rows, null);
  }

  public static void processResult(RESTRequest restRequest, String jsonString,
      ArrayList<String> columns, ArrayList<Object[]> rows, String targetElementName)
          throws JsonParseException, JsonMappingException, IOException {
    if (restRequest != null && restRequest.outputFormat != null
        && restRequest.outputFormat.equalsIgnoreCase("text")) {
      columns.add("Result");
      jsonString = jsonString.replaceAll("\\\\n", "<br>");
      rows.add(new String[] { jsonString });
    } else {
      processResult(jsonString, columns, rows, targetElementName);
    }
  }

  public static void processResult(String jsonString, ArrayList<String> columns,
      ArrayList<Object[]> rows) throws JsonParseException, JsonMappingException, IOException {
    processResult(jsonString, columns, rows, null);
  }

  public static void processResult(String jsonString, ArrayList<String> columns,
      ArrayList<Object[]> rows, String targetElementName)
          throws JsonParseException, JsonMappingException, IOException {
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);
    boolean arrayNodeFound = false;
    JsonNode arrayNode = null;

    try {
      JsonNode node = mapper.readTree(jsonString);

      // If specific target element is specified in request use that, else parse for first array
      // node
      if (targetElementName != null && targetElementName.trim().length() > 0) {
        JsonNode targetNode = node.findValue(targetElementName);
        if (targetNode != null) {
          arrayNode = node;
          if (targetNode instanceof ArrayNode) {
            arrayNodeFound = true;
          }
        }
      } else {
        if (!node.isArray()) {
          if (node instanceof ObjectNode) {
            arrayNode = findArrayNode(node);
            if (arrayNode != null) arrayNodeFound = true;
          }
        } else {
          arrayNode = node;
          arrayNodeFound = true;
        }
      }
      if (arrayNodeFound && arrayNode != null) {
        for (final JsonNode objNode : arrayNode) {
          List<String> rowValues = new ArrayList<String>();
          if (objNode instanceof ArrayNode) {
            arrayNodeFound = true;
            Iterator<JsonNode> valNodes = objNode.elements();
            int i = 1;
            while (valNodes.hasNext()) {
              if (!columns.contains("col" + i)) {
                columns.add("col" + i);
                i++;
              }
              String valueString = valNodes.next().toString();
              if (valueString != null) {
                valueString = valueString.replaceAll("\"", "");
                valueString = valueString.replaceAll("\\\\n", "<br>");
              }
              rowValues.add(valueString);
            }
          } else if (objNode instanceof ObjectNode) {
            Iterator<String> names = objNode.fieldNames();
            while (names.hasNext()) {
              String name = names.next();
              JsonNode value = objNode.get(name);
              if (!columns.contains(name)) {
                columns.add(name);
              }
              String valueString = value.toString();
              if (valueString != null) {
                valueString = valueString.replaceAll("\"", "");
                valueString = valueString.replaceAll("\\\\n", "<br>");
              }
              rowValues.add(valueString);
            }
          }
          if (rowValues.size() > 0) {
            rows.add(rowValues.toArray());
            rowValues.clear();
          }
        }
      } else {
        List<String> rowValues = new ArrayList<String>();
        if (node instanceof ObjectNode) {
          Iterator<Map.Entry<String, JsonNode>> childNodes = node.fields();

          while (childNodes.hasNext()) {
            Map.Entry<String, JsonNode> childNode = childNodes.next();
            if (childNode.getValue().isArray()) {
              arrayNodeFound = true;
              for (final JsonNode objNode : childNode.getValue()) {
                if (objNode instanceof ArrayNode) {
                  Iterator<JsonNode> valNodes = objNode.elements();
                  int i = 1;
                  while (valNodes.hasNext()) {
                    if (!columns.contains("col" + i)) {
                      columns.add("col" + i);
                      i++;
                    }
                    String valueString = valNodes.next().toString();
                    if (valueString != null) {
                      valueString = valueString.replaceAll("\"", "");
                      valueString = valueString.replaceAll("\\\\n", "<br>");
                    }
                    rowValues.add(valueString);
                  }
                } else if (objNode instanceof ObjectNode) {
                  Iterator<String> names = objNode.fieldNames();
                  while (names.hasNext()) {
                    String name = names.next();
                    JsonNode value = objNode.get(name);
                    if (!columns.contains(name)) {
                      columns.add(name);
                    }
                    String valueString = value.toString();
                    if (valueString != null) {
                      valueString = valueString.replaceAll("\"", "");
                      valueString = valueString.replaceAll("\\\\n", "<br>");
                    }
                    rowValues.add(valueString);
                  }
                }

              }
            } else if (!arrayNodeFound && childNode.getValue() instanceof TextNode) {
              String name = childNode.getKey();
              JsonNode value = childNode.getValue();
              if (!columns.contains(name)) {
                columns.add(name);
              }
              String valueString = value.toString();
              if (valueString != null) {
                valueString = valueString.replaceAll("\"", "");
                valueString = valueString.replaceAll("\\\\n", "<br>");
              }
              rowValues.add(valueString);
            }

          }
          if (rowValues.size() > 0) {
            rows.add(rowValues.toArray());
            rowValues.clear();
          }
        }
      }
    } catch (Exception e) {
      // _LOG.error("The Server did not return a valid JSON response. Response string : " +
      // jsonString);
      // throw new RuntimeException("The Server did not return a valid JSON response. Response
      // string : " + jsonString);
      columns.add("Result");
      String[] rowValues = jsonString.split(System.getProperty("line.separator"));
      for (String row : rowValues) {
        rows.add(new String[] { row });
      }
    }
  }

  public static String GetNodeValue(String jsonString, String targetElementName)
      throws JsonParseException, JsonMappingException, IOException {
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    try {
      JsonNode node = mapper.readTree(jsonString);

      // If specific target element is specified in request use that, else parse for first array
      // node
      if (targetElementName != null && targetElementName.trim().length() > 0) {
        JsonNode targetNode = node.findValue(targetElementName);
        if (targetNode != null) {
          return targetNode.toString();
        }
      }
    } catch (Exception ex) {
    }
    return "";
  }
}
