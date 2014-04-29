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
import com.wandisco.s3hdfs.rewrite.redirect.BucketInfoRedirect;
import com.wandisco.s3hdfs.rewrite.redirect.ObjectInfoRedirect;
import com.wandisco.s3hdfs.rewrite.redirect.VersionRedirect;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsResponseWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.WebHdfsResponseWrapper;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import java.io.IOException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

public class DeleteCommand extends Command {

  public DeleteCommand(final S3HdfsRequestWrapper request,
                       final S3HdfsResponseWrapper response,
                       final String serviceHost,
                       final String proxyPort,
                       final S3HdfsPath s3HdfsPath) {
    super(request, response, serviceHost, proxyPort, s3HdfsPath);
  }

  @Override
  protected S3HDFS_COMMAND parseCommand() throws IOException {
    final String query = request.getQueryString();
    final String objectName = s3HdfsPath.getObjectName();
    final String bucketName = s3HdfsPath.getBucketName();

    if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty() &&
        query != null && query.contains("uploadId="))
      return S3HDFS_COMMAND.ABORT_MULTI_PART;
    else if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty() &&
        query != null && query.contains("versionId="))
      return S3HDFS_COMMAND.DELETE_VERSION;
    else if (objectName != null && !objectName.isEmpty() &&
        bucketName != null && !bucketName.isEmpty())
      return S3HDFS_COMMAND.DELETE_OBJECT;
    else if (bucketName != null && !bucketName.isEmpty())
      return S3HDFS_COMMAND.DELETE_BUCKET;
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
      case ABORT_MULTI_PART:
        // A. If we abort a multi part upload then we delete the
        // /root/user/bucket/object/version/upload path.
        modifiedURI = s3HdfsPath.getHdfsRootUploadPath();
        break;
      case DELETE_OBJECT:
        // B. If we delete an object then we delete the
        // /root/user/bucket/object path.
        modifiedURI = s3HdfsPath.getHdfsRootObjectPath();
        break;
      case DELETE_VERSION:
        // C. If we delete a version then we delete the
        // /root/user/bucket/object/version path.
        modifiedURI = s3HdfsPath.getHdfsRootVersionPath();
        break;
      case DELETE_BUCKET:
        // D. If we delete a bucket then we delete the
        // /root/user/bucket path.
        modifiedURI = s3HdfsPath.getHdfsRootBucketPath();
        break;
      default:
        throw new IOException("Unknown command: " + command);
    }

    // 3. Add query to the modified URI.
    modifiedURI += "?user.name=" + s3HdfsPath.getUserName() +
        "&op=DELETE&recursive=true";

    // 4. Set the modified URI.
    modifiedURI = ADD_WEBHDFS(modifiedURI);
    return modifiedURI;
  }

  @Override
  public void doCommand()
      throws ServletException, IOException {

    parseURI();

    // 3. Wrap the response and request so we can manipulate
    final WebHdfsRequestWrapper requestWrap =
        new WebHdfsRequestWrapper(request, command, s3HdfsPath);
    final WebHdfsResponseWrapper responseWrap =
        new WebHdfsResponseWrapper(response);

    RequestDispatcher dispatcher;

    switch (command) {
      // 3,1. Simple delete of versioned object? Create DeleteMarker.
      case DELETE_OBJECT:
        VersionRedirect versionRedirect =
            new VersionRedirect(request, response, s3HdfsPath);
        boolean versioning = versionRedirect.check(s3HdfsPath.getUserName());
        if (versioning) {
          versionRedirect.updateVersion(s3HdfsPath.getUserName());
          versionRedirect.createDeleteMarker(s3HdfsPath.getUserName());
          versionRedirect.createVersion(s3HdfsPath.getUserName());
        }
        break;
      case DELETE_BUCKET:
        BucketInfoRedirect bucketInfoRedirect =
            new BucketInfoRedirect(request, response, s3HdfsPath);
        boolean empty = bucketInfoRedirect.checkEmpty();
        if (!empty) {
          response.setStatus(409);
          response.setContentType("application/xml");
          String filler = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>" +
              "<Code>BucketNotEmpty</Code><Message>" +
              "The bucket you tried to delete is not empty." +
              "</Message></Error>";
          response.setContentLength(filler.length());
          response.getOutputStream().write(filler.getBytes(DEFAULT_CHARSET));
          return;
        }
      default:
        // 3,2. Forward request and obtain response from webHDFS
        dispatcher = requestWrap.getRequestDispatcher(modifiedURI);
        dispatcher.forward(requestWrap, responseWrap);
        break;
    }

    switch (command) {
      case DELETE_VERSION:
        if (response.getStatus() == 200) {
          // Check if we failed to delete version...
          boolean deleteFailure = responseWrap.getContent().contains("false");
          // If we failed deleting a versioned object...
          if (deleteFailure &&
              !s3HdfsPath.getVersion().equals(DEFAULT_VERSION)) {
            // Grab versionId of default object.
            ObjectInfoRedirect objectInfoRedirect =
                new ObjectInfoRedirect(request, response, s3HdfsPath);
            String defaultVersionId = objectInfoRedirect.getContents(
                s3HdfsPath.getHdfsRootObjectPath() + "/" + DEFAULT_VERSION +
                    "/" + VERSION_FILE_NAME
            );
            // If requested versionId equals default versionId...
            if (s3HdfsPath.getVersion().equals(defaultVersionId)) {
              // Delete default version.
              dispatcher = requestWrap.getRequestDispatcher(
                  ADD_WEBHDFS(s3HdfsPath.getHdfsDefaultVersionPath()));
              responseWrap.clearOutputStream();
              dispatcher.forward(requestWrap, responseWrap);
              assert response.getStatus() == 200;

              // Promote last written version to default.
              VersionRedirect versionRedirect =
                  new VersionRedirect(request, response, s3HdfsPath);
              versionRedirect.promoteLatestToDefault();
            }
          }
          response.setHeader("x-amz-version-id", s3HdfsPath.getVersion());
        }
        break;
      default:
        break;
    }

    // 4. Set response for S3 delete
    assert response.getStatus() == 200;
    response.setStatus(204);
    response.setContentLength(0);
    response.setHeader("Connection", "close");
    response.setHeader("Content-Type", null);
  }

}
