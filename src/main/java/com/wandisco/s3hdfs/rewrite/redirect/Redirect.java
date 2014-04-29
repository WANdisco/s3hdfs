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
import com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.*;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

public class Redirect {
  static Logger LOG = LoggerFactory.getLogger(S3HdfsFilter.class);

  final HttpClient httpClient = new HttpClient();

  final HttpServletRequest request;
  final HttpServletResponse response;
  final S3HdfsPath path;

  Redirect(HttpServletRequest request, HttpServletResponse response,
           S3HdfsPath path) {
    this.request = request;
    this.response = response;
    this.path = path;
    httpClient.getParams().setParameter("http.default-headers",
        Arrays.asList(new Header("Connection", "Keep-Alive")));
  }

  String replaceUri(String uri, String target, String replacement) {
    return uri.replace(target, replacement);
  }

  String replaceSrcs(String uri, String srcBucket, String srcObject)
      throws IOException {
    // /root/user/bucket/object/version/.obj
    Path path = new Path(uri);
    String version = path.getParent().getName();
    String user = path.getParent().getParent().getParent().getParent().getName();
    String root = path.getParent().getParent().getParent().getParent().getParent().getName();
    S3HdfsPath newPath = new S3HdfsPath(root, user, srcBucket, srcObject, version, null);
    return ADD_WEBHDFS(newPath.getFullHdfsObjPath());
  }

  HttpMethod getHttpMethod(String scheme, String host, int port, String op,
                           String userName, String uri, HTTP_METHOD method) {

    if (!uri.startsWith(WEBHDFS_PREFIX))
      uri = ADD_WEBHDFS(uri);

    String url = scheme + "://" + host + ":" + port + uri + "?user.name=" +
        userName + "&op=" + op;
    switch (method) {
      case GET:
        return new GetMethod(url);
      case PUT:
        return new PutMethod(url);
      case POST:
        return new PostMethod(url);
      case DELETE:
        return new DeleteMethod(url);
      case HEAD:
        return new HeadMethod(url);
      default:
        return null;
    }
  }

  String readInputStream(InputStream inputStream) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    int result;

    while ((result = inputStream.read()) != -1) {
      buf.write(result);
    }

    return buf.toString(DEFAULT_CHARSET);
  }

}
