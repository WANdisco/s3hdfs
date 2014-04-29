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
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.TestFileCreation;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author shv
 * Do not use any files with more bytes than INTEGER.max length!
 * We use casts in these tests that will overflow!
 */
public class TestBasicUsage extends TestBase {

  @Test
  public void testBasicHDFSConnection() throws IOException, URISyntaxException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "readme.txt");

    assertTrue(hdfs.mkdirs(new Path(s3HdfsPath.getHdfsRootObjectPath())));
    // make blank object file
    FSDataOutputStream fso1 = hdfs.create(new Path(s3HdfsPath.getFullHdfsObjPath()));
    fso1.write('A');
    fso1.close();
    // make blank metadata file
    FSDataOutputStream fso2 = hdfs.create(new Path(s3HdfsPath.getFullHdfsMetaPath()));
    fso2.close();

    // check the file
    URI nnURI = new URI(cluster.getHttpUri(0));
    URL url = new URL(nnURI.getScheme(),
        hostName, PROXY_PORT,
        s3HdfsPath.getS3Path());
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty("Host", "myBucket." + hostName);
    int repCode = connection.getResponseCode();
    System.out.println(connection.getResponseMessage());
    assertEquals(HttpURLConnection.HTTP_OK, repCode);
  }

  @Test
  public void testGetNonExistantBucket()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("rewrite", null);

    try {
      s3Service.listObjects(s3HdfsPath.getBucketName());
    } catch (S3ServiceException exception) {
      assert exception.getResponseCode() == 404;
      assert exception.getS3ErrorCode().equals("NoSuchResource");
    }
  }

  @Test
  public void testGetNonExistantObject()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("rewrite", "object");

    // Without bucket...
    try {
      s3Service.getObject(s3HdfsPath.getBucketName(), s3HdfsPath.getObjectName());
    } catch (S3ServiceException exception) {
      assert exception.getResponseCode() == 404;
      assert exception.getS3ErrorCode().equals("NoSuchResource");
    }

    S3Bucket returnedBucket =
        s3Service.createBucket(s3HdfsPath.getBucketName());
    assertEquals(s3HdfsPath.getBucketName(), returnedBucket.getName());

    // With bucket...
    try {
      s3Service.getObject(s3HdfsPath.getBucketName(), s3HdfsPath.getObjectName());
    } catch (S3ServiceException exception) {
      assert exception.getResponseCode() == 404;
      assert exception.getS3ErrorCode().equals("NoSuchResource");
    }
  }

  @Test
  public void testBasicPutGet()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("rewrite", "readme.txt");

    // Create file and blank metadata in HDFS (but not s3)
    Path path = new Path(s3HdfsPath.getFullHdfsObjPath());
    FSDataOutputStream out =
        TestFileCreation.createFile(hdfs, path, 3);
    TestFileCreation.writeFile(out, 128);
    out.close();

    Path pathMeta = new Path(s3HdfsPath.getFullHdfsMetaPath());
    FSDataOutputStream outMeta = hdfs.create(pathMeta);
    outMeta.close();

    // Get the object
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    S3Object returnedObject1 = s3Service.getObject(bucket.getName(), objectKey);
    System.out.println("RETURNED_OBJECT_1");
    System.out.println(returnedObject1); // returned has dataInputStream!

    // Verify the object
    assertEquals(bucket.getName(), returnedObject1.getBucketName());
    assertEquals(objectKey, returnedObject1.getKey());

    // verify returned data
    testUtil.compareS3ObjectWithHdfsFile(returnedObject1.getDataInputStream(),
        path);

    // List objects
    S3Object[] ls = s3Service.listObjects(bucket.getName());
    assertEquals("Should be one object", 1, ls.length);
    System.out.println("LISTED_OBJECTS_1");
    System.out.println(ls[0]);
  }

  @Test
  public void testMoreBasicPutGet() throws URISyntaxException, ServiceException,
      NoSuchAlgorithmException, IOException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("rewrite", "file1.txt");

    testBasicPutGet();

    // Get the object
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());

    // Put new object
    String object2Key = s3HdfsPath.getObjectName();
    byte[] data = "this is file1.txt".getBytes();
    S3Object object2 = new S3Object(object2Key, data);
    S3Object returnedObject2 = s3Service.putObject(bucket.getName(), object2);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    S3Object returnedObject3 = s3Service.getObject(bucket.getName(), object2Key);
    System.out.println("RETURNED_OBJECT_2");
    System.out.println(returnedObject2); // does not have dataInputStream!
    System.out.println("RETURNED_OBJECT_3");
    System.out.println(returnedObject3); // has dataInputStream!

    // List objects again
    S3Object[] ls2 = s3Service.listObjects(bucket.getName());
    assertEquals("Should be two objects", 2, ls2.length);
    System.out.println("LISTED_OBJECTS_2");
    System.out.println(ls2[0].toString());
    System.out.println(ls2[1].toString());

    // Check paths from HDFS
    FileStatus[] fsa = hdfs.listStatus(
        new Path(s3HdfsPath.getHdfsRootBucketPath()));
    assertEquals(2, fsa.length);
    System.out.println(fsa[0].getPath().toString());
    System.out.println(fsa[1].getPath().toString());

    // Print out HDFS comparison information
    Path path2 = new Path(s3HdfsPath.getFullHdfsObjPath());
    System.out.println(hdfs.listStatus(path2)[0]);
    testUtil.compareS3ObjectWithHdfsFile(returnedObject3.getDataInputStream(),
        path2);
  }

  @Test
  public void testBigPutGet()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[BIG_SIZE];
    for(int i = 0; i < BIG_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }
    S3Object object = new S3Object(objectKey, data);
    S3Object returnedObject2 = s3Service.putObject(bucket.getName(), object);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    System.out.println("RETURNED_OBJECT_2");
    System.out.println(returnedObject2); // does not have dataInputStream!

    S3Object returnedObject1 = s3Service.getObject(bucket.getName(), objectKey);
    System.out.println("RETURNED_OBJECT_1");
    System.out.println(returnedObject1); // returned has dataInputStream!

    // Verify the object
    assertEquals(bucket.getName(), returnedObject1.getBucketName());
    assertEquals(objectKey, returnedObject1.getKey());

    // verify returned data
    testUtil.compareS3ObjectWithHdfsFile(returnedObject1.getDataInputStream(),
        new Path(s3HdfsPath.getFullHdfsObjPath()));

    // List objects
    S3Object[] ls = s3Service.listObjects(bucket.getName());
    assertEquals("Should be one object", 1, ls.length);
    System.out.println("LISTED_OBJECTS_1");
    System.out.println(ls[0]);
  }



  @Test
  public void testDeleteObject()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for(int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }
    S3Object object = new S3Object(objectKey, data);
    S3Object returnedObject2 = s3Service.putObject(bucket.getName(), object);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    System.out.println("RETURNED_OBJECT_2");
    System.out.println(returnedObject2); // does not have dataInputStream!

    S3Object returnedObject1 = s3Service.getObject(bucket.getName(), objectKey);
    System.out.println("RETURNED_OBJECT_1");
    System.out.println(returnedObject1); // returned has dataInputStream!

    // Verify the object
    assertEquals(bucket.getName(), returnedObject1.getBucketName());
    assertEquals(objectKey, returnedObject1.getKey());

    // verify returned data
    testUtil.compareS3ObjectWithHdfsFile(returnedObject1.getDataInputStream(),
        new Path(s3HdfsPath.getFullHdfsObjPath()));

    // List objects
    S3Object[] ls = s3Service.listObjects(bucket.getName());
    assertEquals("Should be one object", 1, ls.length);
    System.out.println("LISTED_OBJECTS_1");
    System.out.println(ls[0]);

    s3Service.deleteObject(bucket.getName(), objectKey);

    // List objects
    S3Object[] ls2 = s3Service.listObjects(bucket.getName());
    assertEquals("Should be one delete marker", 1, ls2.length);
    System.out.println("LISTED_OBJECTS_2");
    System.out.println(ls2[0]);
  }

  @Test
  public void testDeleteBucket()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("deadBucket", null);
    S3HdfsPath s3HdfsPath2 = testUtil.setUpS3HdfsPath("keepAliveBucket", null);

    // Prepare buckets
    S3Bucket bucket1 = new S3Bucket(s3HdfsPath1.getBucketName());
    S3Bucket bucket2 = new S3Bucket(s3HdfsPath2.getBucketName());

    // Create empty bucket
    S3Bucket returnedBucket1 = s3Service.createBucket(bucket1.getName());
    System.out.println("RETURNED_OBJECT_1");
    System.out.println(returnedBucket1);

    // Create another empty bucket
    S3Bucket returnedBucket2 = s3Service.createBucket(bucket2.getName());
    System.out.println("RETURNED_OBJECT_2");
    System.out.println(returnedBucket2);

    // Delete the first bucket
    s3Service.deleteBucket(bucket1.getName());

    // List bucekts
    S3Bucket[] retBuckets = s3Service.listAllBuckets();
    for (S3Bucket retBucket : retBuckets ) {
      System.out.println(retBucket);
    }
    assertEquals(1, retBuckets.length);
    assertEquals(s3HdfsPath2.getBucketName(), retBuckets[0].getName());
  }

  @Test
  public void testListObjects()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {
    S3Bucket myBucket = s3Service.createBucket("myBucket");

    File file = testUtil.getFile(SMALL_SIZE);

    // Prepare buckets
    int randInt = Math.abs(new Random().nextInt() % 10) + 2;
    for(int i = 0; i < randInt; i++) {
      S3Object s3Object = new S3Object("object"+i);
      s3Object.setDataInputFile(file);
      s3Service.putObject(myBucket, s3Object);
    }

    // List objects
    S3Object[] retObjects = s3Service.listObjects(myBucket.getName());
    assert retObjects.length == randInt :
        "Objects created: "+randInt+", objects returned: "+
        Arrays.toString(retObjects);
    for (S3Object retObject : retObjects ) {
      System.out.println(retObject);
    }
  }

  @Test
  public void testListAllBuckets()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {

    // Prepare buckets
    int randInt = Math.abs(new Random().nextInt() % 100) + 10;
    for(int i = 0; i < randInt; i++) {
      S3Bucket bucket = new S3Bucket("myBucket"+i);
      s3Service.createBucket(bucket);
    }

    // List buckets
    S3Bucket[] retBuckets = s3Service.listAllBuckets();
    assert retBuckets.length == randInt :
        "Buckets created: "+randInt+", buckets returned: "+
        Arrays.toString(retBuckets);
    for (S3Bucket retBucket : retBuckets ) {
      System.out.println(retBucket);
    }
  }

  @Test
  public void testMultipleListAllBuckets()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    // S3HdfsFilter.setUserName testing util
    String user1 = "plamenjeliazkov";
    S3HdfsFilter.setFakeUserName(user1);

    // Prepare buckets
    int randInt1 = Math.abs(new Random().nextInt() % 100) + 10;
    for(int i = 0; i < randInt1; i++) {
      S3Bucket bucket = new S3Bucket("myBucket"+i);
      s3Service.createBucket(bucket);
    }

    // List buckets
    S3Bucket[] retBuckets1 = s3Service.listAllBuckets();
    assert retBuckets1.length == randInt1 :
        "Buckets created: "+randInt1+", buckets returned: "+
        Arrays.toString(retBuckets1);
    for (S3Bucket retBucket : retBuckets1) {
      assert retBucket.getOwner().getDisplayName().equals(user1) :
          "Bucket: "+retBucket+", not owned by: "+user1;
      System.out.println(retBucket);
    }

    // S3HdfsFilter.setUserName testing util
    String user2 = "konstantinshvachko";
    S3HdfsFilter.setFakeUserName(user2);

    // Prepare buckets
    int randInt2 = Math.abs(new Random().nextInt() % 100) + 10;
    for(int i = 0; i < randInt2; i++) {
      S3Bucket bucket = new S3Bucket("myBucket"+i);
      s3Service.createBucket(bucket);
    }

    // List buckets
    S3Bucket[] retBuckets2 = s3Service.listAllBuckets();
    assert retBuckets2.length == randInt2 :
        "Buckets created: "+randInt2+", buckets returned: "+
        Arrays.toString(retBuckets2);
    for (S3Bucket retBucket : retBuckets2) {
      assert retBucket.getOwner().getDisplayName().equals(user2) :
          "Bucket: "+retBucket+", not owned by: "+user2;
      System.out.println(retBucket);
    }

    Path s3User1Path = new Path(s3Directory+"/"+user1+"/");
    Path s3User2Path = new Path(s3Directory+"/"+user2+"/");
    FileStatus[] fs = hdfs.listStatus(new Path[]{s3User1Path, s3User2Path});
    assertEquals(randInt1 + randInt2, fs.length);
    S3HdfsFilter.setFakeUserName(null);
  }

  @Test
  public void testByteRangeRead()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    // Put new object
    byte[] data = new byte[SMALL_SIZE];
    for(int i = 0; i < SMALL_SIZE; i++) {
      data[i] = (byte) (i % 256);
    }
    S3Object object = new S3Object(objectKey, data);
    S3Object returnedObject2 = s3Service.putObject(bucket.getName(), object);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    System.out.println("RETURNED_OBJECT_2");
    System.out.println(returnedObject2); // does not have dataInputStream!

    long startRange = 500;
    long endRange = 1000;
    S3Object returnedObject1 =
        s3Service.getObject(bucket.getName(), objectKey, null, null, null, null,
            startRange, endRange);
    System.out.println("RETURNED_OBJECT_1");
    System.out.println(returnedObject1); // returned has dataInputStream!

    // Check paths from HDFS
    FileStatus[] fsa = hdfs.listStatus(
        new Path(s3HdfsPath.getHdfsRootBucketPath()));
    for(FileStatus fs : fsa) {
      System.out.println(fs.getPath());
    }
    assertEquals(1, fsa.length);

    testUtil.compareS3ObjectWithHdfsFile(returnedObject1.getDataInputStream(),
        new Path(s3HdfsPath.getFullHdfsObjPath()), startRange, endRange);
  }

}
