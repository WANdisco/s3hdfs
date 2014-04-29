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

import com.wandisco.s3hdfs.path.S3HdfsPath;
import com.wandisco.s3hdfs.rewrite.redirect.comparator.PartComparator;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.*;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.*;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

/**
 * This class is intended to be used by S3HdfsFilter
 * to deal with the issue of of dealing with multi-part initialization
 * and completion.
 */
public class MultiPartFileRedirect extends Redirect {

  public MultiPartFileRedirect(HttpServletRequest request,
                               HttpServletResponse response,
                               S3HdfsPath path) {
    super(request, response, path);
    LOG.debug("Created " + getClass().getSimpleName() + ".");
  }

  /**
   * Sends a PUT command to create the container directory inside of HDFS.
   * It uses the URL from the original request to do so.
   *
   * @throws IOException
   * @throws ServletException
   */
  public void sendInitiate() throws IOException, ServletException {
    PutMethod httpPut = (PutMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "MKDIRS",
        path.getUserName(), path.getHdfsRootUploadPath(), PUT);

    //Make /root/user/bucket/object/version/upload directory
    httpClient.executeMethod(httpPut);
    httpPut.releaseConnection();
    assert httpPut.getStatusCode() == 200;

    response.setHeader("Set-Cookie",
        httpPut.getResponseHeader("Set-Cookie").getValue());

    // Make /root/user/bucket/object/version/.meta file
    // Set up HttpPut
    httpPut = (PutMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(),
        "CREATE&overwrite=true", path.getUserName(),
        path.getFullHdfsMetaPath(), PUT);

    // Set custom metadata headers
    Enumeration headers = request.getHeaderNames();
    Properties metadata = new Properties();
    while (headers.hasMoreElements()) {
      String key = (String) headers.nextElement();
      if (key.startsWith("x-amz-meta-")) {
        metadata.setProperty(key, request.getHeader(key));
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
    LOG.debug("Contains redirect? " + containsRedirect);

    if (!containsRedirect) {
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

    if (httpPut.getStatusCode() != 200) {
      LOG.debug("Response content: " + httpPut.getResponseBodyAsString());
    }

    // Consume 2nd response, assure it was a 200
    httpPut.releaseConnection();
    assert httpPut.getStatusCode() == 200;
  }

  public void sendComplete() throws IOException, ServletException {
    //STEP 1. Get listing of .part's.
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "LISTSTATUS",
        path.getUserName(), ADD_WEBHDFS(path.getHdfsRootUploadPath()), GET);
    httpClient.executeMethod(httpGet);

    // STEP 2. Parse sources from listing.
    String sourceStr = readInputStream(httpGet.getResponseBodyAsStream());

    List<String> sources = parseSources(path.getHdfsRootUploadPath(),
        sourceStr);
    httpGet.releaseConnection();
    assert httpGet.getStatusCode() == 200;


    //STEP 3. Perform concatenation of other .part's into 1.part.
    if (sources.size() > 1) {
      Collections.sort(sources, new PartComparator(path.getHdfsRootUploadPath()));
      if (!partsAreInOrder(sources)) {
        response.setStatus(400);
        return;
      }
      doIncrementalConcat(sources);
    }

    //STEP 3. Rename concat'd 1.part as .obj
    PutMethod httpPut = (PutMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(),
        "RENAME&destination=" + path.getFullHdfsObjPath(), path.getUserName(),
        ADD_WEBHDFS(path.getHdfsRootUploadPath() + "1" + PART_FILE_NAME), PUT);
    httpClient.executeMethod(httpPut);
    httpPut.releaseConnection();
    assert httpPut.getStatusCode() == 200;

    //STEP 4. Delete upload directory
    DeleteMethod httpDelete = (DeleteMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "DELETE",
        path.getUserName(), path.getHdfsRootUploadPath(), DELETE);
    httpClient.executeMethod(httpDelete);
    httpDelete.releaseConnection();
    assert httpDelete.getStatusCode() == 200;
  }

  private void doIncrementalConcat(List<String> sources)
      throws IOException {
    int sourcesToConcat = sources.size();
    int increments = sourcesToConcat / 500;

    int i = 0;
    do {
      int startIndex = (i * 500 == 0) ? 1 : i * 500; //1, 500, 1000, 1500...
      int endIndex = ((i + 1) * 500);                  //499, 999, 1499...
      if (endIndex >= sourcesToConcat) endIndex = sourcesToConcat;
      List<String> toConcat = sources.subList(startIndex, endIndex);
      System.out.println("CONCAT SRCS[" + i + "]: " + toConcat.toString());
      String conCatSrcs = StringUtils.join(",", toConcat);

      PostMethod httpPost = (PostMethod) getHttpMethod(request.getScheme(),
          request.getServerName(), request.getServerPort(),
          "CONCAT&sources=" + conCatSrcs, path.getUserName(),
          ADD_WEBHDFS(path.getHdfsRootUploadPath() + "1" + PART_FILE_NAME), POST);

      httpClient.executeMethod(httpPost);
      httpPost.releaseConnection();
      assert httpPost.getStatusCode() == 200;

      i++;
    } while (i <= increments);
  }

  private List<String> parseSources(String hdfsRootVersionPath, String sources)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonRoot = mapper.readTree(sources);
    JsonNode array = jsonRoot.get("FileStatuses").get("FileStatus");

    ArrayList<String> retVal = new ArrayList<String>();
    for (int i = 0; i < array.size(); i++) {
      String name;
      JsonNode element = array.get(i);
      name = element.get("pathSuffix").getTextValue();
      if (name.matches("[1-9]+[0-9]*" + PART_FILE_NAME)) {
        retVal.add(hdfsRootVersionPath + name);
      }
    }

    return retVal;
  }

  private boolean partsAreInOrder(List<String> sources) {
    String pathStr = path.getHdfsRootUploadPath();
    int index = 0;

    for (String source : sources) {
      int nextIndex =
          Integer.decode(source.replace(pathStr, "").replace(PART_FILE_NAME, ""));
      if (nextIndex == (index + 1)) {
        index = nextIndex;
      } else {
        return false;
      }
    }
    return true;
  }
}
