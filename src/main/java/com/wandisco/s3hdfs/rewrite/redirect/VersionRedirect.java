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
import com.wandisco.s3hdfs.rewrite.redirect.comparator.DateComparator;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsFileStatus;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.hadoop.util.StringUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.GET;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.PUT;

public class VersionRedirect extends Redirect {

  public VersionRedirect(HttpServletRequest request,
                         HttpServletResponse response,
                         S3HdfsPath path) {
    super(request, response, path);
    LOG.debug("Created " + getClass().getSimpleName() + ".");
  }

  public boolean check(String userName) throws IOException {
    return check(request.getServerName() + ":" + request.getServerPort(),
        userName);
  }

  /**
   * Checks the bucket to see if versioning is enabled or not.
   *
   * @param nnHostAddress
   * @param userName
   * @return
   */
  public boolean check(String nnHostAddress, String userName) throws IOException {
    String[] nnHost = nnHostAddress.split(":");

    String uri = (path != null) ? path.getFullHdfsObjPath() :
        request.getPathInfo();
    List<String> bucketPath = Arrays.asList(uri.split("/")).subList(0, 4);
    String bucketMetaPath =
        StringUtils.join("/", bucketPath) + "/" + BUCKET_META_FILE_NAME;

    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        nnHost[0], Integer.decode(nnHost[1]), "OPEN", userName,
        bucketMetaPath, GET);

    httpClient.executeMethod(httpGet);

    String versionConfXml = readInputStream(httpGet.getResponseBodyAsStream());
    httpGet.releaseConnection();

    if (versionConfXml.contains("<Status>Enabled</Status>")) {
      return true;
    } else if (versionConfXml.contains("<Status>Suspended</Status>")) {
      return false;
    } else {
      return false;
    }
  }

  public void updateVersion(String userName) throws IOException {
    updateVersion(request.getServerName() + ":" + request.getServerPort(),
        userName);
  }

  /**
   * Call this to rename the default version directory to its version file.
   *
   * @throws IOException
   */
  public void updateVersion(String nnHostAddress, String userName)
      throws IOException {
    String[] nnHost = nnHostAddress.split(":");

    String uri = (path != null) ? path.getFullHdfsObjPath() :
        request.getPathInfo();

    String versionpath = replaceUri(uri, OBJECT_FILE_NAME,
        VERSION_FILE_NAME);

    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        nnHost[0], Integer.decode(nnHost[1]), "OPEN", userName,
        versionpath, GET);

    httpClient.executeMethod(httpGet);

    String version = readInputStream(httpGet.getResponseBodyAsStream());
    httpGet.releaseConnection();

    if (httpGet.getStatusCode() == 200) {
      String renameDst =
          replaceUri(uri, DEFAULT_VERSION + "/" + OBJECT_FILE_NAME,
              version);
      String renameSrc =
          replaceUri(uri, DEFAULT_VERSION + "/" + OBJECT_FILE_NAME,
              DEFAULT_VERSION);

      PutMethod httpPut = (PutMethod) getHttpMethod(request.getScheme(),
          nnHost[0], Integer.decode(nnHost[1]), "RENAME&destination=" + renameDst,
          userName, renameSrc, PUT);

      httpClient.executeMethod(httpPut);
      httpPut.releaseConnection();

      assert httpPut.getStatusCode() == 200;
    }
  }

  /**
   * Places a new .d file in the default version directory.
   *
   * @param userName
   * @throws IOException
   */
  public void createDeleteMarker(String userName)
      throws IOException {
    String versionpath = replaceUri(path.getFullHdfsObjPath(), OBJECT_FILE_NAME,
        DELETE_MARKER_FILE_NAME);

    PutMethod httpPut =
        (PutMethod) getHttpMethod(request.getScheme(), request.getServerName(),
            request.getServerPort(), "CREATE&overwrite=true", userName,
            versionpath, PUT);
    httpPut.setRequestHeader(S3_HEADER_NAME, S3_HEADER_VALUE);

    httpClient.executeMethod(httpPut);

    Header locationHeader = httpPut.getResponseHeader("Location");
    LOG.debug("1st response: " + httpPut.getStatusLine().toString());
    boolean containsRedirect = (locationHeader != null);
    httpPut.releaseConnection();

    if (!containsRedirect) {
      LOG.debug("1st response did not contain redirect. " +
          "No version will be created.");
      return;
    }

    // Handle redirect header transition
    assert httpPut.getStatusCode() == 307;

    // Consume response and re-allocate connection for redirect
    httpPut.setURI(new URI(locationHeader.getValue(), true));

    httpClient.executeMethod(httpPut);

    LOG.debug("2nd response: " + httpPut.getStatusLine().toString());

    if (httpPut.getStatusCode() != 200) {
      LOG.debug("Response not 200: " + httpPut.getResponseBodyAsString());
      return;
    }

    assert httpPut.getStatusCode() == 200;
    httpPut.releaseConnection();

    response.setHeader("x-amz-delete-marker", "true");
  }


  public String createVersion(String userName) throws IOException {
    return createVersion(request.getServerName() + ":" + request.getServerPort(),
        userName);
  }

  /**
   * Places a new .v file in the default version directory.
   *
   * @param nnHostAddress
   * @param userName
   * @throws IOException
   */
  public String createVersion(String nnHostAddress, String userName)
      throws IOException {
    String[] nnHost = nnHostAddress.split(":");

    String uri = (path != null) ? path.getFullHdfsObjPath() :
        request.getPathInfo();

    String versionpath = replaceUri(uri, OBJECT_FILE_NAME,
        VERSION_FILE_NAME);

    PutMethod httpPut =
        (PutMethod) getHttpMethod(request.getScheme(), nnHost[0],
            Integer.decode(nnHost[1]), "CREATE&overwrite=true", userName,
            versionpath, PUT);
    String version = UUID.randomUUID().toString();
    httpPut.setRequestEntity(
        new ByteArrayRequestEntity(version.getBytes(DEFAULT_CHARSET)));

    httpPut.setRequestHeader(S3_HEADER_NAME, S3_HEADER_VALUE);

    httpClient.executeMethod(httpPut);

    Header locationHeader = httpPut.getResponseHeader("Location");
    LOG.debug("1st response: " + httpPut.getStatusLine().toString());
    boolean containsRedirect = (locationHeader != null);
    httpPut.releaseConnection();

    if (!containsRedirect) {
      LOG.debug("1st response did not contain redirect. " +
          "No version will be created.");
      return null;
    }

    // Handle redirect header transition
    assert httpPut.getStatusCode() == 307;

    // Consume response and re-allocate connection for redirect
    httpPut.setURI(new URI(locationHeader.getValue(), true));

    httpClient.executeMethod(httpPut);

    LOG.debug("2nd response: " + httpPut.getStatusLine().toString());

    if (httpPut.getStatusCode() != 200) {
      LOG.debug("Response not 200: " + httpPut.getResponseBodyAsString());
      return null;
    }

    assert httpPut.getStatusCode() == 200;
    httpPut.releaseConnection();

    return version;
  }

  public void promoteLatestToDefault() throws IOException, ServletException {

    ObjectInfoRedirect objectInfoRedirect =
        new ObjectInfoRedirect(request, response, path);

    final String objectName = path.getObjectName();

    List<S3HdfsFileStatus> versionList =
        objectInfoRedirect.getFilteredListing(objectName, "");
    objectInfoRedirect.modifyVersionList(objectName, versionList);

    // Remove everything EXCEPT the object we are interested in.
    Collections.sort(versionList, new DateComparator());

    if (versionList.size() > 0) {
      String versionToPromote = versionList.get(versionList.size() - 1)
          .getLocalName();
      String renameDst = path.getHdfsDefaultVersionPath();
      String renameSrc = replaceUri(renameDst, DEFAULT_VERSION, versionToPromote);

      PutMethod httpPut = (PutMethod) getHttpMethod(request.getScheme(),
          request.getServerName(), request.getServerPort(),
          "RENAME&destination=" + renameDst, path.getUserName(), renameSrc, PUT);

      httpClient.executeMethod(httpPut);
      httpPut.releaseConnection();

      assert httpPut.getStatusCode() == 200;
    }
  }

}
