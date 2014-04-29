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
package com.wandisco.s3hdfs.command;

import com.wandisco.s3hdfs.path.S3HdfsPath;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsResponseWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsResponseWrapper;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import java.io.IOException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_CHARSET;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND.*;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

public class PutCommand extends Command {

  public PutCommand(final S3HdfsRequestWrapper request,
                    final S3HdfsResponseWrapper response,
                    final String serviceHost,
                    final String proxyPort,
                    final S3HdfsPath s3HdfsPath) {
    super(request, response, serviceHost, proxyPort, s3HdfsPath);
  }

  @Override
  protected S3HDFS_COMMAND parseCommand()
      throws IOException {
    final String queryString = request.getQueryString();
    final String objectName = s3HdfsPath.getObjectName();
    final String bucketName = s3HdfsPath.getBucketName();

    if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty() &&
        queryString != null && !queryString.isEmpty() &&
        queryString.contains("uploadId=") &&
        queryString.contains("partNumber="))
      return UPLOAD_PART;
    else if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty() &&
        request.getHeader("x-amz-copy-source") != null &&
        !request.getHeader("x-amz-copy-source").isEmpty())
      return COPY_OBJECT;
    else if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty())
      return PUT_OBJECT;
    else if (bucketName != null && !bucketName.isEmpty() &&
        queryString != null && !queryString.isEmpty() &&
        queryString.contains("versioning"))
      return CONFIGURE_VERSIONING;
    else if (bucketName != null && !bucketName.isEmpty())
      return PUT_BUCKET;
    else
      throw new IOException(
          "Unable to parse command. Request: " + request.toString() +
              ", s3HdfsPath: " + s3HdfsPath.toString()
      );
  }

  @Override
  protected String parseURI()
      throws IOException {

    // 1. Parse the command.
    command = parseCommand();

    System.out.println("Command parsed as: " + command.toString());

    // 2. Parse the modified URI.
    switch (command) {
      case UPLOAD_PART:
        // A. If we upload a part then we need the full upload path
        // Full path == /root/user/bucket/object/version/upload/"part"
        modifiedURI = s3HdfsPath.getFullHdfsUploadPartPath();
        break;
      case PUT_OBJECT:
        // B. If we put in an object then we need the full path
        // Full path == /root/user/bucket/object/version/"file"
        modifiedURI = s3HdfsPath.getFullHdfsObjPath();
        break;
      case COPY_OBJECT:
        // C. If we copy an object then we need the full path
        // Full path == /root/user/bucket/object/version/"file"
        modifiedURI = s3HdfsPath.getFullHdfsObjPath();
        break;
      case CONFIGURE_VERSIONING:
        // D. If we are configuring versioning then we need the bucket meta path
        // Bucket meta path = /root/user/bucket/"bucketmeta"
        modifiedURI = s3HdfsPath.getFullHdfsBucketMetaPath();
        break;
      case PUT_BUCKET:
        // D. If we put in a bucket then we need only the bucket path
        // Bucket path == /root/user/bucket/
        modifiedURI = s3HdfsPath.getHdfsRootBucketPath();
        break;
      default:
        throw new IOException("Unknown command: " + command);
    }

    // 3. Set the modified URI.
    modifiedURI = ADD_WEBHDFS(modifiedURI);
    return modifiedURI;
  }

  @Override
  public void doCommand()
      throws ServletException, IOException {

    // Grab the URI to communicate our actions to.
    parseURI();

    // Wrap the response and request so we can manipulate.
    final WebHdfsRequestWrapper requestWrap =
        new WebHdfsRequestWrapper(request, command, s3HdfsPath);
    final WebHdfsResponseWrapper responseWrap =
        new WebHdfsResponseWrapper(response);

    // Forward request and obtain response from webHDFS.
    // (NOTE: This launches another S3HdfsFilter in DataNode usually)
    final RequestDispatcher dispatcher =
        requestWrap.getRequestDispatcher(modifiedURI);
    dispatcher.forward(requestWrap, responseWrap);

    if (response.getStatus() == 307) {
      response.setContentType("application/xml");
      String filler = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>" +
          "<Code>TemporaryRedirect</Code><Message>" +
          "Please re-send this request to the specified temporary endpoint." +
          " S3Hdfs requires that you stream your data into an HDFS DataNode." +
          " Continue to use the original request endpoint for future requests" +
          " and follow the temporary redirect via the 'location' header." +
          "</Message></Error>";
      response.setContentLength(filler.length());
      response.getOutputStream().write(filler.getBytes(DEFAULT_CHARSET));
    }
  }

}
