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
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsFileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_VERSION;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DELETE_MARKER_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.GET;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.OBJECT_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.VERSION_FILE_NAME;

/**
 * This class is intended to be used by S3HdfsFilter
 * to deal with the issue of checking object informations.
 */
public class ObjectInfoRedirect extends Redirect {

  public ObjectInfoRedirect(HttpServletRequest request,
                            HttpServletResponse response,
                            S3HdfsPath path) {
    super(request, response, path);
    LOG.debug("Created "+getClass().getSimpleName()+".");
  }

  /**
   * Sends a GET command to obtain the stat info on the object.
   * It uses the URL from the original request to do so.
   * @throws IOException
   * @throws ServletException
   */
  public Properties getInfo() throws IOException, ServletException {
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "GETFILESTATUS",
        path.getUserName(), path.getFullHdfsObjPath(), GET);

    httpClient.executeMethod(httpGet);

    String properties = readInputStream(httpGet.getResponseBodyAsStream());
    Properties info = parseFileInfo(properties);

    // consume response
    httpGet.releaseConnection();
    assert httpGet.getStatusCode() == 200;
    return info;
  }

  public String getContents(String pathToRead) throws IOException {
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "OPEN",
        path.getUserName(), pathToRead, GET);

    httpClient.executeMethod(httpGet);

    String content = readInputStream(httpGet.getResponseBodyAsStream());

    // consume response
    httpGet.releaseConnection();
    assert httpGet.getStatusCode() == 200;
    return content;
  }

  public S3HdfsFileStatus getStatus(String object, String version)
      throws IOException, ServletException {
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "GETFILESTATUS",
        path.getUserName(),
        path.getHdfsRootBucketPath() + object + "/" + version + "/" + OBJECT_FILE_NAME,
        GET);

    httpClient.executeMethod(httpGet);

    // If file does not exist, check if delete marker for it exists.
    if(httpGet.getStatusCode() == 404) {
      httpGet.releaseConnection();
      httpGet = (GetMethod) getHttpMethod(request.getScheme(),
          request.getServerName(), request.getServerPort(), "GETFILESTATUS",
          path.getUserName(),
          path.getHdfsRootBucketPath() + object + "/" + version + "/" + DELETE_MARKER_FILE_NAME,
          GET);

      httpClient.executeMethod(httpGet);
    }

    String jsonStatus = readInputStream(httpGet.getResponseBodyAsStream());
    JsonNode element = new ObjectMapper().readTree(jsonStatus).get("FileStatus");

    // consume response
    httpGet.releaseConnection();
    assert httpGet.getStatusCode() == 200;
    return parseHdfsFileStatus(element);
  }

  public List<S3HdfsFileStatus> getFilteredListing(String objectName, String prefix)
      throws IOException {

    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "LISTSTATUS",
        path.getUserName(), path.getHdfsRootBucketPath() + objectName, GET);

    httpClient.executeMethod(httpGet);

    String listingStr = readInputStream(httpGet.getResponseBodyAsStream());
    List<S3HdfsFileStatus> listing = parseAndFilterListing(listingStr, prefix);

    // consume response
    httpGet.releaseConnection();
    assert httpGet.getStatusCode() == 200;
    return listing;
  }

  public Map<String, List<S3HdfsFileStatus>> getVersions(List<S3HdfsFileStatus> listing)
      throws IOException, ServletException {
    Map<String, List<S3HdfsFileStatus>> versions =
        new HashMap<String, List<S3HdfsFileStatus>>();

    for(S3HdfsFileStatus stats : listing) {
      String objectName = stats.getLocalName();
      List<S3HdfsFileStatus> versionList = getFilteredListing(objectName, null);
      modifyVersionList(objectName, versionList);
      versions.put(stats.getLocalName(), versionList);
    }

    return versions;
  }

  // Here we replace the version HdfsFileStatus with its length and sizes,
  // but keep the version directory name.
  void modifyVersionList(String objectName,
                         List<S3HdfsFileStatus> versionList)
      throws IOException, ServletException {
    for (S3HdfsFileStatus f : versionList) {
      String versionName = f.getLocalName();

      S3HdfsFileStatus versionedObject = getStatus(objectName, versionName);

      // If this is the DEFAULT_VERSION; get the REAL versionId.
      if (DEFAULT_VERSION.equals(versionName)) {
        GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
            request.getServerName(), request.getServerPort(), "OPEN",
            path.getUserName(),
            path.getHdfsRootBucketPath() + objectName + "/" + DEFAULT_VERSION + "/" + VERSION_FILE_NAME,
            GET);

        httpClient.executeMethod(httpGet);
        versionName = readInputStream(httpGet.getResponseBodyAsStream());
        httpGet.releaseConnection();
        f.setLatest(true);
      } else {
        f.setLatest(false);
      }

      String delMarkerPath = path.getHdfsRootBucketPath() + objectName + "/" +
                             f.getLocalName() + "/" + DELETE_MARKER_FILE_NAME;
      boolean delMarker = checkExists(delMarkerPath);
      f.setDelMarker(delMarker);
      f.setVersionId(versionName);
      f.setObjectModTime(versionedObject.getModificationTime());
      f.setObjectSize(versionedObject.getLen());
    }
  }

  private List<S3HdfsFileStatus> parseAndFilterListing(String versionStr,
                                                       String prefix)
      throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonRoot = mapper.readTree(versionStr);
    JsonNode array = jsonRoot.get("FileStatuses").get("FileStatus");

    List<S3HdfsFileStatus> versions = new ArrayList<S3HdfsFileStatus>();

    for(int i = 0; i < array.size(); i++) {
      JsonNode element = array.get(i);

      String path = element.get("pathSuffix").getTextValue();
      if(prefix != null && !path.startsWith(prefix))
        continue;

      S3HdfsFileStatus status = parseHdfsFileStatus(element);
      versions.add(status);
    }

    return versions;
  }

  private S3HdfsFileStatus parseHdfsFileStatus(JsonNode element) {
    return new S3HdfsFileStatus(
        element.get("length").getLongValue(),
        element.get("type").getTextValue().equalsIgnoreCase("DIRECTORY"),
        element.get("replication").getIntValue(),
        element.get("blockSize").getLongValue(),
        element.get("modificationTime").getLongValue(),
        element.get("accessTime").getLongValue(),
        FsPermission.createImmutable((short) element.get("permission").getIntValue()),
        element.get("owner").getTextValue(),
        element.get("group").getTextValue(),
        (element.get("symlink") == null) ? null :
         DFSUtil.string2Bytes(element.get("symlink").getTextValue()),
        DFSUtil.string2Bytes(element.get("pathSuffix").getTextValue()),
        element.get("fileId").getLongValue(),
        element.get("childrenNum").getIntValue());
  }

  private Properties parseFileInfo(String fileInfo)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonRoot = mapper.readTree(fileInfo);
    JsonNode jsonInfo = jsonRoot.get("FileStatus");
    Properties info = new Properties();
    info.setProperty("Content-Length",
                     Long.toString(jsonInfo.get("length").getLongValue()));
    return info;
  }

  public boolean checkExists(String pathToCheck) throws IOException {
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "GETFILESTATUS",
        path.getUserName(), pathToCheck, GET);

    httpClient.executeMethod(httpGet);
    httpGet.releaseConnection();

    return httpGet.getStatusCode() == 200;
  }

}
