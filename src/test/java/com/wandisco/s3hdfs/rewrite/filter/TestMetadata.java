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
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_VERSION;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.META_FILE_NAME;
import static org.junit.Assert.assertTrue;

public class TestMetadata extends TestBase {

  @Test
  @SuppressWarnings("deprecation")
  public void testBasicMetadataRead()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    NameNode nn = cluster.getNameNode();
    System.out.println(nn.getHttpAddress().toString());

    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for (int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }
    S3Object object = new S3Object(s3HdfsPath.getObjectName(), data);
    Map<String, Object> metaEntries = new HashMap<String, Object>();
    metaEntries.put("scared", "yes");
    metaEntries.put("tired", "yes");
    metaEntries.put("hopeless", "never");
    object.addAllMetadata(metaEntries);
    object.setMetadataComplete(true);
    s3Service.putObject(s3HdfsPath.getBucketName(), object);

    HttpClient httpClient = new HttpClient();

    // Set up HttpGet and get response
    FileStatus fs = hdfs.getFileStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()));
    assertTrue(fs.isFile());
    assertTrue(fs.getPath().getName().equals(META_FILE_NAME));
    String url = "http://" + hostName + ":" + PROXY_PORT +
        "/webhdfs/v1/s3hdfs/" + s3HdfsPath.getUserName() +
        "/myBucket/bigFile/" + DEFAULT_VERSION + "/" + META_FILE_NAME + "?op=OPEN";
    GetMethod httpGet = new GetMethod(url);
    httpClient.executeMethod(httpGet);
    InputStream is = httpGet.getResponseBodyAsStream();
    Properties retVal = testUtil.parseMap(is);
    System.out.println(retVal);

    // consume response and re-allocate connection
    httpGet.releaseConnection();
    assert httpGet.getStatusCode() == 200;
    assert retVal.getProperty("x-amz-meta-scared").equals("yes");
    assert retVal.getProperty("x-amz-meta-tired").equals("yes");
    assert retVal.getProperty("x-amz-meta-hopeless").equals("never");
  }

  @Test
  public void testBasicMetadataCreate()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for (int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }

    S3Object object = new S3Object(objectKey, data);
    Map<String, Object> metaEntries = new HashMap<String, Object>();
    metaEntries.put("scared", "yes");
    metaEntries.put("tired", "yes");
    metaEntries.put("hopeless", "never");
    object.addAllMetadata(metaEntries);
    object.setMetadataComplete(true);
    S3Object returnedObject = s3Service.putObject(bucket.getName(), object);

    testUtil.compareS3ObjectWithHdfsFile(new ByteArrayInputStream(data),
        new Path(s3HdfsPath.getFullHdfsObjPath()));

    S3Object returnedObject2 = s3Service.getObject(bucket.getName(),
        object.getKey());
    System.out.println(returnedObject.getMetadataMap());
    System.out.println(returnedObject);
    System.out.println(returnedObject2.getMetadataMap());
    System.out.println(returnedObject2);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));
    assertTrue(testUtil.checkMetadataEntries(s3HdfsPath));

    Map<String, Object> map = returnedObject2.getMetadataMap();
    assert map.get("scared").equals("yes");
    assert map.get("tired").equals("yes");
    assert map.get("hopeless").equals("never");
    assert map.get("md5-hash") != null;
  }

  @Test
  public void testHeadObjectMetadata()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    File file = testUtil.getFile(SMALL_SIZE);

    S3Object object = new S3Object(objectKey);
    object.setDataInputFile(file);
    Map<String, Object> metaEntries = new HashMap<String, Object>();
    metaEntries.put("scared", "yes");
    metaEntries.put("tired", "yes");
    metaEntries.put("hopeless", "never");
    object.addAllMetadata(metaEntries);
    object.setMetadataComplete(true);
    S3Object returnedObject = s3Service.putObject(bucket.getName(), object);

    testUtil.compareS3ObjectWithHdfsFile(new FileInputStream(file),
        new Path(s3HdfsPath.getFullHdfsObjPath()));

    StorageObject returnedObject2 =
        s3Service.getObjectDetails(s3HdfsPath.getBucketName(),
            s3HdfsPath.getObjectName());

    System.out.println(returnedObject.getMetadataMap());
    System.out.println(returnedObject);
    System.out.println(returnedObject2.getMetadataMap());
    System.out.println(returnedObject2);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));
    assertTrue(testUtil.checkMetadataEntries(s3HdfsPath));

    Map<String, Object> map = returnedObject2.getMetadataMap();
    assert map.get("scared").equals("yes");
    assert map.get("tired").equals("yes");
    assert map.get("hopeless").equals("never");
  }

}
