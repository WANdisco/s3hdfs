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
package com.wandisco.s3hdfs.path;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;

/**
 * This class is a utility designed to create an S3Hdfs path; meaning
 * a path that maps S3 request paths to paths in HDFS.
 */
public class S3HdfsPath {
  private final String rootDir;
  private final String userName;
  private final String bucketName;
  private final String objectName;
  private final String partNumber;

  private String version;

  public S3HdfsPath() throws UnsupportedEncodingException {
    this(null, null, null, null, null, null);
  }

  /**
   * Constructs an S3HdfsPath using known information after entering
   * the S3HdfsFilter. This path represents a /bucket/object in S3 and
   * a /root/user/bucket/object/version/file in HDFS.
   *
   * @param rootDir    the s3Hdfs root dir (req)
   * @param userName   the username behind the current request (req)
   * @param bucketName the s3 bucketname (opt)
   * @param objectName the s3 objectname (opt)
   * @param version    the version requested (opt)
   * @param partNumber the partNumber (opt)
   */
  public S3HdfsPath(final String rootDir, final String userName,
                    final String bucketName, final String objectName,
                    final String version, final String partNumber)
      throws UnsupportedEncodingException {
    this.rootDir = rootDir;
    this.userName = userName;
    this.bucketName = bucketName;
    this.objectName = (objectName == null) ? null :
        URLDecoder.decode(objectName, "UTF-8");
    this.version = version;
    this.partNumber = partNumber;
  }

  /**
   * Returns the /bucket/object path, or the /bucket/ path,
   * if no object was given.
   * Returns empty String if neither were given.
   *
   * @return the s3 path
   */
  public String getS3Path() {
    String retVal = "";
    if (bucketName != null)
      retVal = "/" + bucketName + "/";
    if (objectName != null && !retVal.isEmpty())
      retVal = retVal + objectName;
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version/file path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the full hdfs .obj path
   */
  public String getFullHdfsObjPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + version + "/" + OBJECT_FILE_NAME;
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version/meta path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the full hdfs .meta path
   */
  public String getFullHdfsMetaPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + version + "/" + META_FILE_NAME;
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version/vers path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the full hdfs .obj path
   */
  public String getFullHdfsVersPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + version + "/" + VERSION_FILE_NAME;
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version/file path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the full hdfs .obj path
   */
  public String getFullHdfsDeleteMarkerPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + version + "/" + DELETE_MARKER_FILE_NAME;
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket path,
   * or the /root/user/ path if no bucket was given.
   *
   * @return the hdfs bucket dir path
   */
  public String getFullHdfsBucketMetaPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/" + BUCKET_META_FILE_NAME;
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version/upload path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the hdfs upload dir path
   */
  public String getHdfsRootUploadPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + version + "/" + UPLOAD_DIR_NAME + "/";
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the hdfs version dir path
   */
  public String getHdfsRootVersionPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + version + "/";
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the hdfs version dir path
   */
  public String getHdfsDefaultVersionPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName + "/" + DEFAULT_VERSION + "/";
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object path,
   * or the /root/user/bucket/ path if no object was given.
   * <p/>
   * Returns /root/user/ if no bucket, object and version were given.
   *
   * @return the hdfs object dir path
   */
  public String getHdfsRootObjectPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty())
        retVal = retVal + objectName;
    }
    return retVal;
  }

  /**
   * Returns the /root/user/bucket path,
   * or the /root/user/ path if no bucket was given.
   *
   * @return the hdfs bucket dir path
   */
  public String getHdfsRootBucketPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
    }
    return retVal;
  }

  /**
   * Returns the /root/user/ path.
   *
   * @return the hdfs user dir path
   */
  public String getHdfsRootUserPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";
    return retVal;
  }

  /**
   * Returns the /root/user/bucket/object/version/upload/part path
   *
   * @return the full hdfs .part path
   */
  public String getFullHdfsUploadPartPath() {
    String retVal;
    if (rootDir.charAt(0) == '/')
      retVal = rootDir + "/" + userName + "/";
    else
      retVal = "/" + rootDir + "/" + userName + "/";

    if (!bucketName.isEmpty()) {
      retVal = retVal + bucketName + "/";
      if (!objectName.isEmpty() && !version.isEmpty()) {
        retVal = retVal + objectName + "/" + version + "/" + UPLOAD_DIR_NAME + "/" +
            partNumber + PART_FILE_NAME;
      }
    }
    return retVal;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getObjectName() {
    return objectName;
  }

  public String getUserName() {
    return userName;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getPartNumber() {
    return partNumber;
  }

  public String toString() {
    return "[bucket=" + getBucketName() + ",object=" + getObjectName() + ",user=" +
        getUserName() + ",version=" + getVersion() + ",part=" + getPartNumber() + "]";
  }

}
