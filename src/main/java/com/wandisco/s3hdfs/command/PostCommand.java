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
import com.wandisco.s3hdfs.rewrite.redirect.MultiPartFileRedirect;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsResponseWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsResponseWrapper;
import com.wandisco.s3hdfs.rewrite.xml.S3XmlWriter;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import java.io.IOException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

public class PostCommand extends Command {

  public PostCommand(final S3HdfsRequestWrapper request,
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
        queryString != null && !queryString.contains("uploadId"))
      return S3HDFS_COMMAND.INITIATE_MULTI_PART;
    else if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty() &&
        queryString != null && queryString.contains("uploadId"))
      return S3HDFS_COMMAND.COMPLETE_MULTI_PART;
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
    switch (command) {
      case INITIATE_MULTI_PART:
        // A. If we initiate a multipart upload then we use
        // Version path == /root/user/bucket/object/version/upload
        modifiedURI = s3HdfsPath.getHdfsRootUploadPath();
        break;
      case COMPLETE_MULTI_PART:
        // B. If we complete a multipart upload then we use
        // Version path == /root/user/bucket/object/version/upload
        modifiedURI = s3HdfsPath.getHdfsRootUploadPath();
        break;
      default:
        throw new IOException("Unknown command: " + command);
    }
    modifiedURI = ADD_WEBHDFS(modifiedURI);
    return modifiedURI;
  }

  @Override
  public void doCommand()
      throws IOException, ServletException {

    parseURI();

    // 1. Wrap the response and request so we can manipulate
    final WebHdfsRequestWrapper requestWrap =
        new WebHdfsRequestWrapper(request, command, s3HdfsPath);
    final WebHdfsResponseWrapper responseWrap =
        new WebHdfsResponseWrapper(response);

    final MultiPartFileRedirect redirect =
        new MultiPartFileRedirect(requestWrap, responseWrap, s3HdfsPath);

    S3XmlWriter xmlWriter;
    String xml;

    switch (command) {
      // 2,1. Initiating a multi-part? Create upload dir and metadata file.
      case INITIATE_MULTI_PART:
        redirect.sendInitiate();
        if (response.getStatus() == 200) {
          response.setContentType("application/xml");
          xmlWriter = new S3XmlWriter(null, s3HdfsPath.getUserName());
          xml = xmlWriter.writeMultiPartInitiateToXml(
              s3HdfsPath.getBucketName(), s3HdfsPath.getObjectName());
          response.setContentLength(xml.length());
          response.getOutputStream().write(xml.getBytes("UTF-8"));
        }
        break;
      // 2,2. Completing a multi-part? Concat and delete upload dir.
      case COMPLETE_MULTI_PART:
        redirect.sendComplete();
        response.setContentType("application/xml");
        if (response.getStatus() == 200) {
          xmlWriter = new S3XmlWriter(null, s3HdfsPath.getUserName());
          xml = xmlWriter.writeMultiPartCompleteToXml(
              s3HdfsPath.getBucketName(), s3HdfsPath.getObjectName(),
              serviceHostName, proxyPort);
          response.setContentLength(xml.length());
          response.getOutputStream().write(xml.getBytes("UTF-8"));
        } else if (response.getStatus() == 400) {
          response.getOutputStream().write((
              "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                  "<Error><Code>InvalidPart</Code>" +
                  "<Message>One or more of the specified parts could not be found. " +
                  "The part might not have been uploaded, or the specified entity tag " +
                  "might not have matched the part's entity tag.</Message>" +
                  "<RequestId>HARDCODED0123456789</RequestId>" +
                  "<HostId>HARDCODED0123456789</HostId>" +
                  "</Error>").getBytes("UTF-8"));
        }
        break;
      default:
        final RequestDispatcher dispatcher =
            requestWrap.getRequestDispatcher(modifiedURI);
        dispatcher.forward(requestWrap, responseWrap);
        break;
    }
  }

}
