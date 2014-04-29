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
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.BaseVersionOrDeleteMarker;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3BucketVersioningStatus;
import org.jets3t.service.model.S3Object;
import org.junit.Test;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestVersioning extends TestBase {

  @Test
  public void testEnableVersioning() throws IOException, ServiceException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("versionedBucket");

    // Enable versioning
    S3Bucket retBucket =
        s3Service.createBucket(new S3Bucket(s3HdfsPath1.getBucketName()));

    s3Service.enableBucketVersioning(retBucket.getName());

    // Check versioning is enabled
    S3BucketVersioningStatus bucketVersioningStatus =
        s3Service.getBucketVersioningStatus(retBucket.getName());
    assertEquals(true, bucketVersioningStatus.isVersioningEnabled());

    s3Service.suspendBucketVersioning(retBucket.getName());

    // Check versioning is disabled
    bucketVersioningStatus =
        s3Service.getBucketVersioningStatus(retBucket.getName());
    assertEquals(false, bucketVersioningStatus.isVersioningEnabled());
  }

  @Test
  public void testBasicVersionedPut()
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

    FileStatus defaultObj = hdfs.getFileStatus(new Path(s3HdfsPath1.getFullHdfsObjPath()));
    FileStatus versionObj;

    if(fs[0].getPath().getName().equals(DEFAULT_VERSION)) {
      assertTrue(fs[1].getPath().getName().matches("[0-9A-z.-]+"));
      s3HdfsPath1.setVersion(fs[1].getPath().getName());
    } else {
      assertTrue(fs[0].getPath().getName().matches("[0-9A-z.-]+"));
      assertEquals(DEFAULT_VERSION, fs[1].getPath().getName());
      s3HdfsPath1.setVersion(fs[0].getPath().getName());
    }

    versionObj = hdfs.getFileStatus(new Path(s3HdfsPath1.getFullHdfsObjPath()));
    assertEquals(SMALL_SIZE, defaultObj.getLen());
    assertEquals(BIG_SIZE, versionObj.getLen());
  }

  @Test
  public void testGetByVersionIds()
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

    S3Object defaultObj = s3Service.getVersionedObject(DEFAULT_VERSION,
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());
    S3Object versionObj = s3Service.getVersionedObject(s3HdfsPath1.getVersion(),
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    System.out.println(defaultObj.toString());
    System.out.println(versionObj.toString());
  }

  @Test
  public void testDeleteVersion()
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

    S3Object ret1 = s3Service.putObject(s3HdfsPath1.getBucketName(), object1);
    s3Service.putObject(s3HdfsPath1.getBucketName(), object2);

    FileStatus[] fs =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(2, fs.length);

    s3Service.deleteVersionedObject(ret1.getVersionId(),
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(1, fs2.length);
  }

  @Test
  public void testDeleteMarkerCreation()
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

    s3Service.deleteObject(s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    try {
      s3Service.getObject(s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());
    } catch (S3ServiceException e) {
      assertNotNull(e.getResponseHeaders().get("x-amz-delete-marker"));
      assertEquals("true", e.getResponseHeaders().get("x-amz-delete-marker"));
    }

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(3, fs2.length);
  }

  @Test
  public void testDeleteMarkerDeletionAndPromotion()
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

    s3Service.deleteObject(s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(3, fs2.length);

    BaseVersionOrDeleteMarker[] versions =
        s3Service.getObjectVersions(s3HdfsPath1.getBucketName(),
                                    s3HdfsPath1.getObjectName());

    for(BaseVersionOrDeleteMarker version : versions) {
      System.out.println(version.toString());
      if(version.isDeleteMarker()) {
        // This will delete the latest version, causing promotion to occur.
        s3Service.deleteVersionedObject(version.getVersionId(),
            s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());
      }
    }

    FileStatus[] fs3 =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(2, fs3.length);
  }

  @Test
  public void testListVersions()
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

    BaseVersionOrDeleteMarker[] versions = s3Service.getObjectVersions(
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    System.out.println(versions[0].toString());
    System.out.println(versions[1].toString());
  }

  @Test
  public void testListManyVersionsAndPromotion()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("folder", "file1");

    // Enable versioning
    S3Bucket retBucket =
        s3Service.createBucket(new S3Bucket(s3HdfsPath1.getBucketName()));

    s3Service.enableBucketVersioning(retBucket.getName());

    // Put new object
    File file1 = testUtil.getFile(SMALL_SIZE);
    File file2 = testUtil.getFile(BIG_SIZE);

    int randomInt = Math.abs(new Random().nextInt() % 20) + 10;

    for(int i = 0; i < randomInt; i++) {
      S3Object object = new S3Object(s3HdfsPath1.getObjectName());
      object.setDataInputFile(file1);
      s3Service.putObject(s3HdfsPath1.getBucketName(), object);
      // Need a tiny sleep to let DataNode reciever threads clear up.
      Thread.sleep(20);
    }

    S3Object object = new S3Object(s3HdfsPath1.getObjectName());
    object.setDataInputFile(file2);
    s3Service.putObject(s3HdfsPath1.getBucketName(), object);

    FileStatus[] fs =
        hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(randomInt + 1, fs.length);

    BaseVersionOrDeleteMarker[] versions = s3Service.getObjectVersions(
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    String defaultVersion = null;
    String secondLatestId = null;
    long secondHighestModTime = -1;

    for(BaseVersionOrDeleteMarker version : versions) {
      System.out.println(version.toString());
      if(version.isLatest()) {
        defaultVersion = testUtil.readInputStream(
            hdfs.open(new Path(s3HdfsPath1.getFullHdfsVersPath())));
        assertEquals(defaultVersion, version.getVersionId());
        assertTrue(version.toString().contains("size="+BIG_SIZE));
      } else {
        assertTrue(version.toString().contains("size="+SMALL_SIZE));
        long modTime = version.getLastModified().getTime();
        if(modTime > secondHighestModTime) {
          secondLatestId = version.getVersionId();
          secondHighestModTime = modTime;
        }
      }
    }

    // This will delete the default version, causing promotion to occur.
    s3Service.deleteVersionedObject(defaultVersion, s3HdfsPath1.getBucketName(),
                                    s3HdfsPath1.getObjectName());

    fs = hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootObjectPath()));
    assertEquals(randomInt, fs.length);

    versions = s3Service.getObjectVersions(
        s3HdfsPath1.getBucketName(), s3HdfsPath1.getObjectName());

    for(BaseVersionOrDeleteMarker version : versions) {
      System.out.println(version.toString());
      assertTrue(version.toString().contains("size="+SMALL_SIZE));
      if(version.isLatest()) {
        assertEquals(secondLatestId, version.getVersionId());
        assertEquals(secondHighestModTime, version.getLastModified().getTime());
      }
    }
  }

}
