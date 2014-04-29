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
import com.wandisco.s3hdfs.rewrite.redirect.ObjectInfoRedirect;
import com.wandisco.s3hdfs.rewrite.wrapper.*;
import com.wandisco.s3hdfs.rewrite.xml.S3XmlWriter;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND.*;
import static com.wandisco.s3hdfs.rewrite.filter.S3HdfsFilter.ADD_WEBHDFS;

public class GetCommand extends Command {

  public GetCommand(final S3HdfsRequestWrapper request,
                    final S3HdfsResponseWrapper response,
                    final String serviceHost,
                    final String proxyPort,
                    final S3HdfsPath s3HdfsPath) {
    super(request, response, serviceHost, proxyPort, s3HdfsPath);
  }

  @Override
  protected S3HDFS_COMMAND parseCommand() throws IOException {
    final String queryString = request.getQueryString();
    final String objectName = s3HdfsPath.getObjectName();
    final String bucketName = s3HdfsPath.getBucketName();

    if ((objectName == null || objectName.isEmpty()) &&
        (bucketName == null || bucketName.isEmpty()))
      return GET_ALL_BUCKETS;
    else if (bucketName != null && !bucketName.isEmpty() &&
        objectName != null && !objectName.isEmpty() &&
        queryString != null && queryString.contains("uploadId="))
      return LIST_PARTS;
    else if (bucketName != null && !bucketName.isEmpty() &&
        queryString != null && queryString.contains("versions") &&
        (objectName == null || objectName.isEmpty()))
      return LIST_VERSIONS;
    else if (objectName != null && !objectName.isEmpty())
      return GET_OBJECT;
    else if (bucketName != null && !bucketName.isEmpty() &&
        queryString != null && !queryString.isEmpty() &&
        queryString.contains("versioning"))
      return GET_VERSIONING;
    else if (bucketName != null && !bucketName.isEmpty())
      return GET_BUCKET;
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
      case GET_ALL_BUCKETS:
        // A. If we get all buckets we need the user path
        // User path == /root/user/
        modifiedURI = s3HdfsPath.getHdfsRootUserPath();
        break;
      case LIST_PARTS:
        // B. If we get all the parts we need the root upload path
        // Upload path == /root/user/bucket/object/version/upload/
        modifiedURI = s3HdfsPath.getHdfsRootUploadPath();
        break;
      case LIST_VERSIONS:
        // B. If we get all the parts we need the root upload path
        // Upload path == /root/user/bucket/object/version/upload/
        modifiedURI = s3HdfsPath.getHdfsRootBucketPath();
        break;
      case GET_VERSIONING:
        // C. If we get versioning configuration we need bucket meta path
        // Bucket meta path == /root/user/bucket/"bucketmeta"
        modifiedURI = s3HdfsPath.getFullHdfsBucketMetaPath();
        break;
      case GET_BUCKET:
        // D. If we get a bucket we need the bucket path
        // Bucket path == /root/user/bucket/
        modifiedURI = s3HdfsPath.getHdfsRootBucketPath();
        break;
      case GET_OBJECT:
        // E. If we get an object we need the full path
        // Full path == /root/user/bucket/object/version/"file"
        modifiedURI = s3HdfsPath.getFullHdfsObjPath();
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

    // Forward request and obtain response from webHDFS
    // (NOTE: This launches in another S3HdfsFilter in DataNode usually)
    RequestDispatcher dispatcher =
        requestWrap.getRequestDispatcher(modifiedURI);
    dispatcher.forward(requestWrap, responseWrap);

    if (response.getStatus() == 404) {

      // If we were getting a versioned object...
      if (!s3HdfsPath.getVersion().equals(DEFAULT_VERSION)) {
        // Check the default version!
        s3HdfsPath.setVersion(DEFAULT_VERSION);
        dispatcher = requestWrap.getRequestDispatcher(ADD_WEBHDFS(
            s3HdfsPath.getFullHdfsObjPath()));
        responseWrap.clearOutputStream();
        dispatcher.forward(requestWrap, responseWrap);
      } else {
        ObjectInfoRedirect objectInfoRedirect =
            new ObjectInfoRedirect(request, response, s3HdfsPath);
        boolean deleteMarker =
            objectInfoRedirect.checkExists(s3HdfsPath.getFullHdfsDeleteMarkerPath());
        if (deleteMarker)
          response.setHeader("x-amz-delete-marker", "true");
      }

      if (response.getStatus() == 404) {
        response.setStatus(404);
        response.setContentType("application/xml");
        switch (command) {
          case LIST_PARTS:
            response.getOutputStream().write(("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<Error><Code>NoSuchUpload</Code>" +
                "<Message>The specified multipart upload does not exist." +
                " The upload ID might be invalid, or the multipart upload might" +
                " have been aborted or completed.</Message>" +
                "<Resource>" + request.getParameter("uploadId") + "</Resource>" +
                "<RequestId>HARDCODED0123456789</RequestId></Error>").getBytes(DEFAULT_CHARSET));
            break;
          case GET_ALL_BUCKETS:
            /* This just means there are no buckets in hdfs so its ok to return 200 and just no buckets */
            S3XmlWriter xmlWriter = new S3XmlWriter(null, s3HdfsPath.getUserName());
            response.setStatus(200);
            response.getOutputStream().write(xmlWriter.writeNoneBucketsToXml().getBytes());
            break;
          default:
            response.getOutputStream().write(("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<Error><Code>NoSuchResource</Code>" +
                "<Message>The resource you requested could not be found.</Message>" +
                "<Resource>" + s3HdfsPath.getS3Path() + "</Resource>" +
                "<RequestId>HARDCODED0123456789</RequestId></Error>").getBytes(DEFAULT_CHARSET));
            break;
        }
        return;
      }
    }

    // convert HDFS jsonObject into s3 XML
    final String content = responseWrap.getContent();
    if (content != null && !content.isEmpty()) {
      final S3XmlWriter xmlWriter = new S3XmlWriter(content,
          s3HdfsPath.getUserName());
      String xml;
      switch (command) {
        case GET_ALL_BUCKETS:
          xml = xmlWriter.writeAllMyBucketsToXml();
          break;
        case GET_BUCKET:
          xml = xmlWriter.writeListBucketToXml(s3HdfsPath.getBucketName());
          break;
        case LIST_PARTS:
          xml = xmlWriter.writeListPartsToXml(s3HdfsPath.getBucketName(),
              s3HdfsPath.getObjectName(), s3HdfsPath.getUserName(),
              request.getParameter("uploadId"),
              request.getParameter("part-number-marker"),
              request.getParameter("max-parts"));
          break;
        case LIST_VERSIONS:
          String prefix = request.getParameter("prefix");
          String bucket = s3HdfsPath.getBucketName();
          ObjectInfoRedirect objectInfoRedirect =
              new ObjectInfoRedirect(request, response, s3HdfsPath);
          List<S3HdfsFileStatus> listing = objectInfoRedirect.getFilteredListing("", prefix);
          Map<String, List<S3HdfsFileStatus>> versions = objectInfoRedirect.getVersions(listing);
          xml = xmlWriter.writeListVersionsToXml(versions, bucket);
          break;
        default:
          throw new IOException("Unknown response to construct for: " + command);
      }

      // write the converted content into the original response
      response.setContentType("application/xml");
      response.getOutputStream().write(xml.getBytes("UTF-8"));
    }
  }

}
