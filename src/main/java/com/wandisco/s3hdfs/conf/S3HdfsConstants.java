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
package com.wandisco.s3hdfs.conf;

import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;

/**
 * The list of constants used throughout S3HDFS.
 */
public interface S3HdfsConstants {

  /**
   * The current version of S3HDFS
   */
  public static final String S3_VERSION =
      "4.0";

  /**
   * The default charset for byte conversion
   */
  public static final String DEFAULT_CHARSET =
      "UTF-8";

  /**
   * The file name for the object metadata file in S3Hdfs.
   */
  public static final String META_FILE_NAME =
      ".m";

  /**
   * The file name for the bucket metadata file in S3Hdfs.
   */
  public static final String BUCKET_META_FILE_NAME =
      ".b";

  /**
   * The file name for the object file in S3Hdfs.
   */
  public static final String OBJECT_FILE_NAME =
      ".o";

  /**
   * The file name for the part file in S3Hdfs.
   */
  public static final String PART_FILE_NAME =
      ".p";

  /**
   * The file name for the metadata file in S3Hdfs.
   */
  public static final String VERSION_FILE_NAME =
      ".v";

  /**
   * The file name for the delete marker file in S3Hdfs.
   */
  public static final String DELETE_MARKER_FILE_NAME =
      ".d";

  /**
   * The directory name for the upload directory in S3Hdfs.
   */
  public static final String UPLOAD_DIR_NAME =
      "upload";

  /**
   * The directory name for the default version directory in S3Hdfs.
   */
  public static final String DEFAULT_VERSION =
      "default";

  /**
   * The header name for the S3HDFS special header applied by S3HdfsProxy.
   */
  public static final String S3_HEADER_NAME =
      "S3HDFS";

  /**
   * The header value for the S3HDFS special header applied by S3HdfsProxy.
   */
  public static final String S3_HEADER_VALUE =
      "true";

  /**
   * The header value for the S3HDFS special header when doing a check.
   */
  public static final String S3_CHECK_URI =
      "/s3hdfs/check";

  /**
   * The prefix to webHDFS for forwarding requests.
   * "/webhdfs/v1"
   */
  public static final String WEBHDFS_PREFIX =
      WebHdfsFileSystem.PATH_PREFIX;

  /**
   * The key for assigning a root directory for the S3Hdfs cluster.
   */
  public static final String S3_DIRECTORY_KEY =
      "s3hdfs.root.directory";

  /**
   * Default value for the root directory of the S3Hdfs cluster.
   */
  public static final String S3_DIRECTORY_DEFAULT =
      "/s3hdfs";

  /**
   * The key for assigning a port for the S3HdfsProxy service.
   */
  public static final String S3_PROXY_PORT_KEY =
      "s3hdfs.proxy.port";

  /**
   * Default port for the S3HdfsProxy service.
   */
  public static final String S3_PROXY_PORT_DEFAULT =
      String.valueOf(80);

  /**
   * Property for defining the service host name for the S3HdfsFilter.
   */
  public static final String S3_SERVICE_HOSTNAME_KEY =
      "s3hdfs.service.host";

  /**
   * Property for defining the service host name for the S3HdfsFilter.
   */
  public static final String S3_SERVICE_HOSTNAME_DEFAULT =
      "localhost";

  /**
   * Property key for s3 max connections
   */
  public static final String S3_MAX_CONNECTIONS_KEY =
      "s3hdfs.proxy.max_connections";

  /**
   * Default value for s3hdfs.proxy.max_connections
   */
  public static final String S3_MAX_CONNECTIONS_DEFAULT =
      String.valueOf(20);

  /**
   * A constant enum that breaks up the four basic S3 requests.
   */
  public static enum HTTP_METHOD {
    GET("GET"),
    PUT("PUT"),
    POST("POST"),
    DELETE("DELETE"),
    HEAD("HEAD");

    private final String text;

    private HTTP_METHOD(final String text) {
      this.text = text;
    }

    /**
     * Returns the String arg constructed with.
     */
    @Override
    public String toString() {
      return text;
    }
  }

  /**
   * A constant enum that breaks up the type of S3Hdfs commands.
   */
  public static enum S3HDFS_COMMAND {
    HEAD_OBJECT, GET_OBJECT, GET_BUCKET,
    GET_VERSIONING, LIST_PARTS, LIST_VERSIONS, GET_ALL_BUCKETS, PUT_OBJECT,
    COPY_OBJECT, UPLOAD_PART, CONFIGURE_VERSIONING, PUT_BUCKET, DELETE_OBJECT,
    DELETE_VERSION, ABORT_MULTI_PART, DELETE_BUCKET, INITIATE_MULTI_PART,
    COMPLETE_MULTI_PART, UNKNOWN
  }

}
