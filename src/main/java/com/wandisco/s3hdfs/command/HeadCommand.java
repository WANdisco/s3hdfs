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
import com.wandisco.s3hdfs.rewrite.redirect.MetadataFileRedirect;
import com.wandisco.s3hdfs.rewrite.redirect.ObjectInfoRedirect;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsResponseWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsResponseWrapper;
import java.io.IOException;
import java.util.Properties;
import javax.servlet.ServletException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

public class HeadCommand extends Command {

  public HeadCommand(final S3HdfsRequestWrapper request,
                     final S3HdfsResponseWrapper response,
                     final String serviceHost,
                     final String proxyPort,
                     final S3HdfsPath s3HdfsPath) {
    super(request, response, serviceHost, proxyPort, s3HdfsPath);
  }

  @Override
  protected S3HDFS_COMMAND parseCommand() throws IOException {
    final String objectName = s3HdfsPath.getObjectName();
    final String bucketName = s3HdfsPath.getBucketName();

    if((objectName != null && !objectName.isEmpty()) &&
       (bucketName != null && !bucketName.isEmpty()))
      return S3HDFS_COMMAND.HEAD_OBJECT;
    else
      throw new IOException("Unknown command: " + command);
  }

  @Override
  protected String parseURI()
      throws IOException {

    // 1. Parse the command.
    command = parseCommand();

    System.out.println("Command parsed as: " + command.toString());

    // 2. Parse the modified URI.
    switch(command) {
      case HEAD_OBJECT:
        // A. If we get object head we need the version path
        // Version path == /root/user/bucket/object/version
        modifiedURI = s3HdfsPath.getHdfsRootVersionPath();
        break;
      default:
        throw new IOException("Unknown command: " + command);
    }

    // 3. Set the modified URI.
    modifiedURI = ADD_WEBHDFS(modifiedURI);
    return modifiedURI;
  }

  @Override
  public void doCommand() throws ServletException, IOException {

    parseURI();

    // Wrap the response and request so we can manipulate
    final WebHdfsRequestWrapper requestWrap =
        new WebHdfsRequestWrapper(request, command, s3HdfsPath);
    final WebHdfsResponseWrapper responseWrap =
        new WebHdfsResponseWrapper(response);

    ObjectInfoRedirect objectInfoRedirect =
        new ObjectInfoRedirect(requestWrap, responseWrap, s3HdfsPath);
    Properties fileInfo = objectInfoRedirect.getInfo();
    if(fileInfo != null) {
      for(String key : fileInfo.stringPropertyNames()) {
        response.setHeader(key, fileInfo.getProperty(key));
      }
      System.out.println("Returning metadata: " + fileInfo);
    }

    MetadataFileRedirect metadataFileRedirect =
        new MetadataFileRedirect(request);
    Properties metadata =
        metadataFileRedirect.sendHeadRead(s3HdfsPath.getFullHdfsMetaPath(),
        serviceHostName + ":" + proxyPort, s3HdfsPath.getUserName());
    if(metadata != null) {
      for(String key : metadata.stringPropertyNames()) {
        response.setHeader(key, metadata.getProperty(key));
      }
      System.out.println("Returning metadata: " + metadata);
    }
  }

}
