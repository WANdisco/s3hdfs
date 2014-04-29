/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wandisco.s3hdfs.rewrite.redirect;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.GET;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.PUT;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.META_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.OBJECT_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_HEADER_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_HEADER_VALUE;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

/**
 * This class is intended to be used by S3HdfsFilter
 * to deal with the issue of creating a metadata Properties file to be
 * associated with the object.
 *
 * This code resides before and after a 307 redirect within the top of
 * S3HdfsFilter and deals specifically with URIs that are already destinationed
 * for webHDFS.
 */
public class MetadataFileRedirect extends Redirect {

  public MetadataFileRedirect(HttpServletRequest request) {
    super(request, null, null);
    LOG.debug("Created " + getClass().getSimpleName() + ".");
  }

  /**
   * Sends a PUT command to create an empty file inside of HDFS.
   * It uses the URL from the original request to do so.
   * It will then consume the 307 response and write to the DataNode as well.
   * The data is "small" in hopes that this will be relatively quick.
   * @throws IOException
   * @throws ServletException
   */
  public void sendCreate(String nnHostAddress, String userName)
      throws IOException, ServletException {
     // Set up HttpPut
    String[] nnHost = nnHostAddress.split(":");
    String metapath = replaceUri(request.getRequestURI(), OBJECT_FILE_NAME,
        META_FILE_NAME);

    PutMethod httpPut =
        (PutMethod) getHttpMethod(request.getScheme(), nnHost[0],
            Integer.decode(nnHost[1]), "CREATE&overwrite=true", userName,
            metapath, PUT);
    Enumeration headers = request.getHeaderNames();
    Properties metadata = new Properties();

    // Set custom metadata headers
    while (headers.hasMoreElements()) {
      String key = (String) headers.nextElement();
      if(key.startsWith("x-amz-meta-")) {
        String value = request.getHeader(key);
        metadata.setProperty(key, value);
      }
    }
    // Include lastModified header
    SimpleDateFormat rc228 = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
    String modTime = rc228.format(Calendar.getInstance().getTime());
    metadata.setProperty("Last-Modified", modTime);

    // Store metadata headers into serialized HashMap in HttpPut entity.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    metadata.store(baos, null);

    httpPut.setRequestEntity(new ByteArrayRequestEntity(baos.toByteArray()));
    httpPut.setRequestHeader(S3_HEADER_NAME, S3_HEADER_VALUE);

    httpClient.executeMethod(httpPut);
    LOG.debug("1st response: " + httpPut.getStatusLine().toString());

    boolean containsRedirect = (httpPut.getResponseHeader("Location") != null);

    if(!containsRedirect) {
      httpPut.releaseConnection();
      LOG.error("1st response did not contain redirect. " +
          "No metadata will be created.");
      return;
    }

    // Handle redirect header transition
    assert httpPut.getStatusCode() == 307;
    Header locationHeader = httpPut.getResponseHeader("Location");
    httpPut.setURI(new URI(locationHeader.getValue(), true));

    // Consume response and re-allocate connection for redirect
    httpPut.releaseConnection();
    httpClient.executeMethod(httpPut);

    LOG.debug("2nd response: " + httpPut.getStatusLine().toString());

    if(httpPut.getStatusCode() != 200) {
       LOG.debug("Response not 200: " + httpPut.getResponseBodyAsString());
      return;
    }

    assert httpPut.getStatusCode() == 200;
  }

  /**
   * Sends a GET command to read a metadata file inside of HDFS.
   * It uses the URL from the original request to do so.
   * It will then consume the 307 response and read from the DataNode as well.
   * @throws IOException
   * @throws ServletException
   */
  public Properties sendRead(String nnHostAddress, String userName)
      throws IOException, ServletException {
    // Set up HttpGet and get response
    String[] nnHost = nnHostAddress.split(":");
    String metapath = replaceUri(request.getRequestURI(), OBJECT_FILE_NAME,
        META_FILE_NAME);

    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        nnHost[0], Integer.decode(nnHost[1]), "OPEN", userName,
        metapath, GET);

    // Try up to 5 times to get the metadata
    httpClient.executeMethod(httpGet);
    LOG.debug("1st response: " + httpGet.getStatusLine().toString());

    for(int i=0; i<5 && httpGet.getStatusCode()==403; i++) {
      httpGet.releaseConnection();
      httpClient.executeMethod(httpGet);
      LOG.debug("Next response: " + httpGet.getStatusLine().toString());
    }
    assert httpGet.getStatusCode() == 200;

    // Read metadata map
    InputStream is = httpGet.getResponseBodyAsStream();
    Properties metadata = new Properties();
    metadata.load(is);

    // Consume response remainder to re-allocate connection and return map
    httpGet.releaseConnection();
    return metadata;
  }

  /**
   * Sends a GET command to read a metadata file inside of HDFS.
   * It uses the URL from the modified URI to do so.
   * It will then consume the 307 response and read from the DataNode as well.
   * @throws IOException
   */
  public Properties sendHeadRead(String metadataPath, String nnHostAddress,
                                 String userName) throws IOException {
    // Set up HttpGet and get response
    String[] nnHost = nnHostAddress.split(":");
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        nnHost[0], Integer.decode(nnHost[1]), "OPEN", userName,
        ADD_WEBHDFS(metadataPath), GET);

    // Try up to 5 times to get the metadata
    httpClient.executeMethod(httpGet);
    LOG.info("1st response: " + httpGet.toString());
    System.out.println("1st response: " + httpGet.toString());

    for(int i=0; i<5 && httpGet.getStatusCode()==403; i++) {
      httpGet.releaseConnection();
      httpClient.executeMethod(httpGet);
      LOG.info("Next response: " + httpGet.toString());
      System.out.println("Next response: " + httpGet.toString());
    }
    assert httpGet.getStatusCode() == 200;

    // Read metadata map
    InputStream is = httpGet.getResponseBodyAsStream();
    Properties metadata = new Properties();
    metadata.load(is);

    // Consume response remainder to re-allocate connection and return map
    httpGet.releaseConnection();
    return metadata;
  }

}
