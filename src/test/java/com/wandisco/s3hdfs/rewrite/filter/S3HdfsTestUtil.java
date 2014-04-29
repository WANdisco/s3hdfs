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
package com.wandisco.s3hdfs.rewrite.filter;

import com.wandisco.s3hdfs.path.S3HdfsPath;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

import java.io.*;
import java.util.Properties;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_VERSION;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.META_FILE_NAME;
import static org.junit.Assert.*;

public class S3HdfsTestUtil {
  private final FileSystem hdfs;
  private final String s3Directory;

  public S3HdfsTestUtil(FileSystem fs, String root) {
    this.hdfs = fs;
    this.s3Directory = root;
  }

  Properties parseMap(InputStream inputStream) throws IOException {
    Properties metadata = new Properties();
    metadata.load(inputStream);
    return metadata;
  }

  String readInputStream(InputStream inputStream)
      throws IOException {
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    int result = bis.read();
    while (result != -1) {
      byte b = (byte) result;
      buf.write(b);
      result = bis.read();
    }
    return buf.toString();
  }

  S3HdfsPath setUpS3HdfsPath(String bucketName)
      throws IOException {
    return setUpS3HdfsPath(bucketName, null);
  }

  S3HdfsPath setUpS3HdfsPath(String bucketName, String objectName)
      throws IOException {
    return setUpS3HdfsPath(bucketName, objectName, null);
  }

  S3HdfsPath setUpS3HdfsPath(String bucketName, String objectName,
                             String userName)
      throws IOException {
    return setUpS3HdfsPath(bucketName, objectName, userName, null, null);
  }

  S3HdfsPath setUpS3HdfsPath(String bucketName, String objectName,
                             String userName, String version,
                             String partNumber)
      throws IOException {
    String user = (userName == null) ?
        UserGroupInformation.getCurrentUser().getShortUserName() : userName;

    String vers = (version == null) ? DEFAULT_VERSION : null;

    return new S3HdfsPath(s3Directory, user, bucketName, objectName,
        vers, partNumber);
  }

  boolean checkMetadataCreated(S3HdfsPath s3HdfsPath)
      throws IOException {
    String metaPath = s3HdfsPath.getFullHdfsMetaPath();
    FileStatus[] fsa = hdfs.listStatus(new Path(metaPath));
    assertEquals(1, fsa.length);
    assertTrue(0 != fsa[0].getLen());
    assertEquals(META_FILE_NAME, fsa[0].getPath().getName());
    return true;
  }

  boolean checkMetadataEntries(S3HdfsPath s3HdfsPath) {
    String metaPath = s3HdfsPath.getFullHdfsMetaPath();

    FSDataInputStream fis;
    String mapStr = null;
    try {
      fis = hdfs.open(new Path(metaPath));
      int size = fis.available();
      byte[] bytes = new byte[size];
      for (int i = 0; i < size; i++) {
        bytes[i] = (byte) fis.read();
      }
      mapStr = new String(bytes);
    } catch (IOException e) {
      fail();
    }

    assertTrue(mapStr.length() != 0);
    return true;
  }

  S3Service configureS3Service(String host, int proxy)
      throws IOException, S3ServiceException {
    // configure the service
    Jets3tProperties props = new Jets3tProperties();
    props.setProperty("s3service.disable-dns-buckets", String.valueOf(true));
    props.setProperty("s3service.s3-endpoint", host);
    props.setProperty("s3service.s3-endpoint-http-port",
        String.valueOf(proxy));
    props.setProperty("s3service.https-only", String.valueOf(false));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AWSCredentials creds =
        new AWSCredentials(ugi.getShortUserName(), "SomeSecretKey", ugi.getUserName());
    return new RestS3Service(creds, null, null, props);
  }

  void compareS3ObjectWithHdfsFile(InputStream objectStream, Path path)
      throws IOException, ServiceException {
    FileStatus fsStat = hdfs.listStatus(path)[0];
    int expectedSize = (int) fsStat.getLen();
    compareS3ObjectWithHdfsFile(objectStream, path, 0, expectedSize);
  }

  void compareS3ObjectWithHdfsFile(InputStream objectStream, Path path,
                                   long rangeStart, long rangeEnd)
      throws IOException, ServiceException {
    FileStatus fsStat = hdfs.listStatus(path)[0];
    int expectedSize = (int) (rangeEnd - rangeStart);
    int blockSize = (int) fsStat.getBlockSize();
    int blocks = (int) Math.ceil((double) expectedSize / (double) blockSize);

    DataInputStream origStream = hdfs.open(path);
    assertEquals(origStream.skip(rangeStart), rangeStart);

    int size = 0;

    for (int i = 0; i < expectedSize; i++) {
      int A = origStream.read();
      int B = objectStream.read();
      if (A == -1 || B == -1)
        fail("Premature end of steam.");
      if (A != B) {
        fail("ERROR: Byte A: " + A + " Byte B: " + B + ", at offset: " + size);
      }
      size++;
    }
    if (size != expectedSize) {
      fail("Incorrect size: " + size + ", expected: " + expectedSize);
    }

    System.out.println("File: " + path + " has " + blocks + " blocks.");
    System.out.println("File: " + path + " has " + blockSize + " blockSize.");
    System.out.println("File: " + path + " has " + expectedSize + " length.");

    System.out.println("SUCCESS! The files match up!");
  }

  File getFile(int size) throws IOException {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) (i % 256);
    }

    File file = new File("src/test/java/resources/test" + size + "File");
    if (file.exists()) {
      assertEquals(true, file.delete());
    }
    assertEquals(true, file.createNewFile());
    FileUtils.writeByteArrayToFile(file, data);
    return file;
  }
}
