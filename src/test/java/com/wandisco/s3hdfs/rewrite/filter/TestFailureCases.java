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
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import static org.junit.Assert.fail;
import org.junit.Test;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_VERSION;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.META_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.OBJECT_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.VERSION_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFailureCases extends TestBase {

  @Test
  public void testSameObjectInSameBucketCreation() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "sameFile");

    // Enable versioning
    S3Bucket bucket =
        s3Service.createBucket(new S3Bucket(s3HdfsPath.getBucketName()));

    s3Service.enableBucketVersioning(bucket.getName());

    // Prepare the object and bucket
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for(int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }

    // Should overwrite the exisiting object and pass!!
    S3Object object = new S3Object(objectKey, data);
    s3Service.putObject(bucket.getName(), object);
    s3Service.putObject(bucket.getName(), object);

    data = new byte[SMALL_SIZE/2];
    for(int i = 0; i < SMALL_SIZE/2; i++) {
      data[i] = (byte) (i % 256);
    }
    object = new S3Object(objectKey, data);

    s3Service.putObject(bucket.getName(), object);

    FileStatus[] fsa = hdfs.listStatus(
        new Path(s3HdfsPath.getHdfsRootVersionPath()));
    assertEquals(3, fsa.length);
    assertEquals(META_FILE_NAME, fsa[0].getPath().getName());
    assertEquals(OBJECT_FILE_NAME, fsa[1].getPath().getName());
    assertEquals(VERSION_FILE_NAME, fsa[2].getPath().getName());
    assertEquals(SMALL_SIZE/2, fsa[1].getLen());

    String version =
        testUtil.readInputStream(hdfs.open(
            new Path(s3HdfsPath.getFullHdfsVersPath())));
    System.out.println(version);
  }

  @Test
  public void testNonEmptyBucketDeletion() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "someFile");

    // Enable versioning
    S3Bucket bucket =
        s3Service.createBucket(new S3Bucket(s3HdfsPath.getBucketName()));

    // Prepare the object and bucket
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for(int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }

    // Should overwrite the exisiting object and pass!!
    S3Object object = new S3Object(objectKey, data);
    s3Service.putObject(bucket.getName(), object);
    s3Service.putObject(bucket.getName(), object);

    data = new byte[SMALL_SIZE/2];
    for(int i = 0; i < SMALL_SIZE/2; i++) {
      data[i] = (byte) (i % 256);
    }
    object = new S3Object(objectKey, data);

    s3Service.putObject(bucket.getName(), object);
    try {
      s3Service.deleteBucket(s3HdfsPath.getBucketName());
      fail();
    } catch (ServiceException e) {
      assertEquals(409, e.getResponseCode());
      assertTrue(e.getErrorCode().contains("BucketNotEmpty"));
    }
  }

  @Test
  public void testDeleteObjectMultipleTimesWithVersioning() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Enable versioning
    S3Bucket bucket =
        s3Service.createBucket(new S3Bucket(s3HdfsPath.getBucketName()));

    s3Service.enableBucketVersioning(bucket.getName());

    // Prepare the object and bucket
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for(int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }
    S3Object object = new S3Object(objectKey, data);
    s3Service.putObject(bucket.getName(), object);

    // These should create delete markers.
    s3Service.deleteObject(bucket.getName(), object.getName());
    s3Service.deleteObject(bucket.getName(), object.getName());
    s3Service.deleteObject(bucket.getName(), object.getName());

    FileStatus[] fs =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootObjectPath()));

    for(FileStatus f : fs) {
      System.out.println(f.toString());
    }

    assertEquals(4, fs.length);
  }

  @Test
  public void testMulitPartAbortAndRetry() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "multifile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());

    byte[] data1 = new byte[1024];
    for(int i = 0; i < 1024; i++) {
      data1[i] = (byte) i;
    }
    S3Object object1 = new S3Object(s3HdfsPath.getObjectName(), data1);

    byte[] data2 = new byte[1024];
    for(int i = 0; i < 1024; i++) {
      data2[i] = (byte) i;
    }
    S3Object object2 = new S3Object(s3HdfsPath.getObjectName(), data2);

    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);
    MultipartPart part1 = s3Service.multipartUploadPart(upload, 1, object1);
    upload.addMultipartPartToUploadedList(part1);
    MultipartPart part2 = s3Service.multipartUploadPart(upload, 2, object2);
    upload.addMultipartPartToUploadedList(part2);

    s3Service.multipartAbortUpload(upload);

    byte[] data3 = new byte[1024];
    for(int i = 0; i < 1024; i++) {
      data1[i] = (byte) i;
    }
    S3Object object3 = new S3Object(s3HdfsPath.getObjectName(), data3);

    upload =
        s3Service.multipartStartUpload(bucket.getName(), object3.getName(), null);
    part1 = s3Service.multipartUploadPart(upload, 1, object3);
    upload.addMultipartPartToUploadedList(part1);

    s3Service.multipartCompleteUpload(upload);

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootVersionPath()));
    assertEquals(2, fs2.length);
    assertTrue(fs2[0].getPath().getName().equals(META_FILE_NAME));
    assertTrue(!fs2[0].isDirectory());
    assertTrue(fs2[1].getPath().getName().equals(OBJECT_FILE_NAME));
    assertEquals(1024, fs2[1].getLen());
    assertTrue(!fs2[1].isDirectory());
  }

  @Test
  public void testMultipleSameMulitPartInitializesAndCloses() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "multifile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    S3Object object1 = new S3Object(s3HdfsPath.getObjectName(), "");

    s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);
    s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);

    //Close the last upload
    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);
    s3Service.multipartCompleteUpload(upload);

    //Close multiple times
    try {
      s3Service.multipartCompleteUpload(upload);
    } catch (S3ServiceException e) {
      assertEquals("NoSuchUpload", e.getS3ErrorCode());
    }
    try {
      s3Service.multipartCompleteUpload(upload);
    } catch (S3ServiceException e) {
      assertEquals("NoSuchUpload", e.getS3ErrorCode());
    }
  }

  @Test
  public void testMulitPartMissingUpload() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "multifile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());

    byte[] data1 = new byte[1024];
    for(int i = 0; i < 1024; i++) {
      data1[i] = (byte) i;
    }
    S3Object object1 = new S3Object(s3HdfsPath.getObjectName(), data1);

    byte[] data3 = new byte[1024];
    for(int i = 0; i < 1024; i++) {
      data1[i] = (byte) i;
    }
    S3Object object3 = new S3Object(s3HdfsPath.getObjectName(), data3);

    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);

    s3Service.multipartUploadPart(upload, 1, object1);
    s3Service.multipartUploadPart(upload, 3, object3);

    try {
      s3Service.multipartCompleteUpload(upload);
    } catch (S3ServiceException e) {
      assertEquals("InvalidPart", e.getS3ErrorCode());
    }
  }

  @Test
  public void testMultipleSameBucketCreation() throws Exception {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", null);

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());

    s3Service.createBucket(bucket);
    s3Service.createBucket(bucket);
    s3Service.createBucket(bucket);

    S3Bucket[] buckets = s3Service.listAllBuckets();
    assertEquals(1, buckets.length);
    assertEquals(s3HdfsPath.getBucketName(), buckets[0].getName());
  }

  @Test
  public void testGetDefaultObjectByVersion()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("folder", "file1");

    // Enable versioning
    S3Bucket retBucket =
        s3Service.createBucket(new S3Bucket(s3HdfsPath1.getBucketName()));

    s3Service.enableBucketVersioning(retBucket.getName());

    // Put new object
    File file1 = testUtil.getFile(BIG_SIZE);
    File file2 = testUtil.getFile(SMALL_SIZE);

    S3Object object1 = new S3Object(s3HdfsPath1.getObjectName());
    object1.setDataInputFile(file1);
    S3Object object2 = new S3Object(s3HdfsPath1.getObjectName());
    object2.setDataInputFile(file2);

    s3Service.putObject(s3HdfsPath1.getBucketName(), object1);
    s3Service.putObject(s3HdfsPath1.getBucketName(), object2);

    FileStatus[] fs =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(2, fs.length);

    if(fs[0].getPath().getName().equals(DEFAULT_VERSION)) {
      assertTrue(fs[1].getPath().getName().matches("[0-9A-z.-]+"));
      s3HdfsPath1.setVersion(fs[1].getPath().getName());
    } else {
      assertTrue(fs[0].getPath().getName().matches("[0-9A-z.-]+"));
      assertEquals(DEFAULT_VERSION, fs[1].getPath().getName());
      s3HdfsPath1.setVersion(fs[0].getPath().getName());
    }

    DataInputStream data = hdfs.open(new Path(
        s3HdfsPath1.getHdfsRootObjectPath() + "/" + DEFAULT_VERSION + "/" +
        VERSION_FILE_NAME));

    String defaultVersion = testUtil.readInputStream(data);
    System.out.println("Default version: "+defaultVersion);

    S3Object defaultObj = s3Service.getVersionedObject(defaultVersion,
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());
    S3Object versionObj = s3Service.getVersionedObject(s3HdfsPath1.getVersion(),
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    System.out.println(defaultObj.toString());
    System.out.println(versionObj.toString());
  }
}



