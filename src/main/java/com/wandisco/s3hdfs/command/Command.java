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
import java.io.IOException;
import javax.servlet.ServletException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND;

/**
 * Command is an interface for GET, PUT, POST, HEAD, and DELETE commands.
 */
public abstract class Command {
  final S3HdfsPath s3HdfsPath;
  final S3HdfsRequestWrapper request;
  final S3HdfsResponseWrapper response;
  final String serviceHostName;
  final String proxyPort;

  S3HDFS_COMMAND command;
  String modifiedURI;

  public static Command make(final S3HdfsRequestWrapper request,
                             final S3HdfsResponseWrapper response,
                             final String serviceHost,
                             final String proxyPort,
                             final String bucketName,
                             final String objectName,
                             final String userName,
                             final String rootDir,
                             final String version,
                             final String partNumber)
      throws IOException, ServletException {

    final S3HdfsPath s3HdfsPath =
        new S3HdfsPath(rootDir, userName, bucketName,
                       objectName, version, partNumber);

    switch (HTTP_METHOD.valueOf(request.getMethod())) {
      case GET:
        return new GetCommand(request, response, serviceHost,
                              proxyPort, s3HdfsPath);
      case PUT:
        return new PutCommand(request, response, serviceHost,
                              proxyPort, s3HdfsPath);
      case POST:
        return new PostCommand(request, response, serviceHost,
                               proxyPort, s3HdfsPath);
      case DELETE:
        return new DeleteCommand(request, response, serviceHost,
                                 proxyPort, s3HdfsPath);
      case HEAD:
        return new HeadCommand(request, response, serviceHost,
                               proxyPort, s3HdfsPath);
      default:
        throw new IOException("Unknown request method: " + request.getMethod());
    }
  }

  public Command(final S3HdfsRequestWrapper request,
                 final S3HdfsResponseWrapper response,
                 final String serviceHost,
                 final String proxyPort,
                 final S3HdfsPath s3HdfsPath) {
    this.request = request;
    this.response = response;
    this.s3HdfsPath = s3HdfsPath;
    this.serviceHostName = serviceHost;
    this.proxyPort = proxyPort;
  }

  /**
   * This is used to parse out which of the available commands in S3HDFS are
   * we actually performing. This is specific to which implemented Command we
   * are using.
   * It is called first in parseURI().
   * Ex. GetCommand only knows the GET_OBJECT, GET_BUCKET, and GET_ALL_BUCKETS
   * operations.
   * @return an S3HDFS_COMMAND enum signifying the operation requested
   * @throws IOException
   */
  protected abstract S3HDFS_COMMAND parseCommand()
      throws IOException;

  /**
   * This is used to get the URI for which we are communicating to.
   * It is called first in doCommand().
   * @return the URI string we are going to forward any requests to
   * @throws IOException
   */
  protected abstract String parseURI()
      throws IOException;

  /**
   * Based on the parsed command and the URI; perform the command!
   * @throws IOException
   * @throws ServletException
   */
  public abstract void doCommand() throws IOException, ServletException;

}
