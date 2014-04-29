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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMultiPartUploads extends TestBase {

  @Test
  public void testWebHDFSConcat()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {
    S3HdfsPath s3HdfsPath1 =
        testUtil.setUpS3HdfsPath("myBucket", "readme.txt", null, null, "1");

    assertTrue(hdfs.mkdirs(new Path(s3HdfsPath1.getHdfsRootObjectPath())));
    // make blank object file
    FSDataOutputStream fso1 = hdfs.create(new Path(s3HdfsPath1.getFullHdfsUploadPartPath()));
    for (int i = 0; i < 1024; i++) {
      fso1.write('A');
    }
    fso1.close();

    S3HdfsPath s3HdfsPath2 =
        testUtil.setUpS3HdfsPath("myBucket", "readme.txt", null, null, "2");

    assertTrue(hdfs.mkdirs(new Path(s3HdfsPath2.getHdfsRootObjectPath())));
    // make blank object file
    FSDataOutputStream fso2 = hdfs.create(new Path(s3HdfsPath2.getFullHdfsUploadPartPath()));
    for (int i = 0; i < 1024; i++) {
      fso2.write('B');
    }
    fso2.close();

    FileStatus[] fsa = hdfs.listStatus(
        new Path(s3HdfsPath1.getHdfsRootUploadPath()));
    assertEquals(2, fsa.length);
    assertEquals(fsa[0].getPath().getName(), "1" + PART_FILE_NAME);
    assertEquals(fsa[1].getPath().getName(), "2" + PART_FILE_NAME);

    String userName = s3HdfsPath1.getUserName();

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "POST",
        "http://" + hostName + ":" + PROXY_PORT + "/webhdfs/v1/" +
            "s3hdfs/" + userName + "/myBucket/readme.txt/" + DEFAULT_VERSION +
            "/upload/1" + PART_FILE_NAME + "?op=CONCAT&sources=/s3hdfs/" + userName +
            "/myBucket/readme.txt/" + DEFAULT_VERSION + "/upload/2" + PART_FILE_NAME
    );
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    fsa = hdfs.listStatus(new Path(s3HdfsPath1.getHdfsRootUploadPath()));
    assertEquals(1, fsa.length);
    assertEquals(fsa[0].getPath().getName(), "1" + PART_FILE_NAME);
  }

  @Test
  public void testZeroLengthMultiPartUpload()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "multifile");

    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket(s3HdfsPath.getBucketName());
    String objectKey = s3HdfsPath.getObjectName();

    // Initiate and complete the upload with no data written.
    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), objectKey, null);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    FileStatus[] fs1 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootVersionPath()));
    System.out.println(Arrays.toString(fs1));

    assertEquals(2, fs1.length);
    assertTrue(fs1[0].getPath().getName().equals(META_FILE_NAME));
    assertTrue(!fs1[0].isDirectory());
    assertTrue(fs1[1].getPath().getName().equals("upload"));
    assertTrue(fs1[1].isDirectory());

    MultipartCompleted multipartCompleted =
        s3Service.multipartCompleteUpload(upload);

    System.out.println(multipartCompleted.toString());

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootVersionPath()));
    assertTrue(fs2.length == 1);
    assertTrue(fs1[0].getPath().getName().equals(META_FILE_NAME));
  }

  @Test
  public void testOneLengthMultiPartUpload()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket("myBucket");

    byte[] data = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      data[i] = (byte) i;
    }
    S3Object object = new S3Object("multifile", data);

    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), object.getName(), null);
    s3Service.multipartUploadPart(upload, 1, object);

    S3HdfsPath s3HdfsPath =
        testUtil.setUpS3HdfsPath("myBucket", "multifile", null, null, "1");
    FileStatus[] fs1 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootUploadPath()));
    assertTrue(fs1[0].getPath().getName().equals("1" + PART_FILE_NAME));

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    MultipartCompleted multipartCompleted =
        s3Service.multipartCompleteUpload(upload);

    System.out.println(multipartCompleted.toString());

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
  public void testTwoLengthMultiPartUpload()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket("myBucket");

    byte[] data1 = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      data1[i] = (byte) i;
    }
    S3Object object1 = new S3Object("multifile", data1);

    byte[] data2 = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      data2[i] = (byte) i;
    }
    S3Object object2 = new S3Object("multifile", data2);

    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);
    MultipartPart part1 = s3Service.multipartUploadPart(upload, 1, object1);
    upload.addMultipartPartToUploadedList(part1);
    MultipartPart part2 = s3Service.multipartUploadPart(upload, 2, object2);
    upload.addMultipartPartToUploadedList(part2);

    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "multifile");
    FileStatus[] fs1 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootUploadPath()));
    assertEquals(2, fs1.length);
    assertTrue(fs1[0].getPath().getName().equals("1" + PART_FILE_NAME));
    assertTrue(fs1[1].getPath().getName().equals("2" + PART_FILE_NAME));
    assertEquals(1024, fs1[0].getLen());
    assertEquals(1024, fs1[1].getLen());

    MultipartCompleted multipartCompleted =
        s3Service.multipartCompleteUpload(upload, Arrays.asList(part1, part2));

    System.out.println(multipartCompleted.toString());

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootVersionPath()));
    assertEquals(2, fs2.length);
    assertTrue(fs2[0].getPath().getName().equals(META_FILE_NAME));
    assertTrue(!fs2[0].isDirectory());
    assertTrue(fs2[1].getPath().getName().equals(OBJECT_FILE_NAME));
    assertEquals(1024 * 2, fs2[1].getLen());
    assertTrue(!fs2[1].isDirectory());
  }

  @Test
  public void testListMultiPartUploads()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("multibucket", "multifile");

    // Initiate multipart
    MultipartUpload upload =
        s3Service.multipartStartUpload(s3HdfsPath.getBucketName(),
            s3HdfsPath.getObjectName(), null);
    // Sorted List
    List<MultipartPart> localParts = new ArrayList<MultipartPart>();

    int numOfParts = Math.abs(new Random().nextInt() % 100) + 10;

    for (int i = 0; i < numOfParts; i++) {
      byte[] data = new byte[1024];
      for (int j = 0; j < 1024; j++) {
        byte bits = (byte) i;
        data[j] = bits;
      }
      S3Object object = new S3Object("multifile", data);
      MultipartPart part = s3Service.multipartUploadPart(upload, i + 1, object);
      // Need a tiny sleep to let DataNode reciever threads clear up.
      Thread.sleep(20);
      upload.addMultipartPartToUploadedList(part);
      localParts.add(part);
    }

    // Unsorted List
    List<MultipartPart> uploadedParts = s3Service.multipartListParts(upload);
    for (int i = 0; i < numOfParts; i++) {
      MultipartPart uploadedPart = uploadedParts.get(i);
      System.out.println(uploadedPart.toString());

      int partNum = uploadedPart.getPartNumber();
      MultipartPart localPart = localParts.get(partNum - 1);

      assertEquals(localPart.getPartNumber(), uploadedPart.getPartNumber());
      assertEquals(localParts.get(i).getSize(), uploadedPart.getSize());
      assertEquals(localParts.get(i).getEtag(), uploadedPart.getEtag());
    }
  }

  @Test
  public void testLargeMultiPartUpload()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("multibucket", "multifile");

    // Initiate multipart
    MultipartUpload upload =
        s3Service.multipartStartUpload(s3HdfsPath.getBucketName(),
            s3HdfsPath.getObjectName(), null);

    int numOfParts = 2000;

    File file = testUtil.getFile(numOfParts * 1024);
    if (file.exists()) {
      assertEquals(true, file.delete());
    }
    assertEquals(true, file.createNewFile());
    FileOutputStream fos = new FileOutputStream(file);

    for (int i = 0; i < numOfParts; i++) {
      byte[] data = new byte[1024];
      for (int j = 0; j < 1024; j++) {
        byte bits = (byte) i;
        data[j] = bits;
        fos.write(bits);
      }
      S3Object object = new S3Object("multifile", data);
      MultipartPart part = s3Service.multipartUploadPart(upload, i + 1, object);
      // Need a tiny sleep to let DataNode reciever threads clear up.
      Thread.sleep(20);
      upload.addMultipartPartToUploadedList(part);
    }

    MultipartCompleted multipartCompleted =
        s3Service.multipartCompleteUpload(upload);

    System.out.println(multipartCompleted.toString());

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootVersionPath()));
    assertEquals(2, fs2.length);
    assertTrue(fs2[0].getPath().getName().equals(META_FILE_NAME));
    assertTrue(!fs2[0].isDirectory());
    assertTrue(fs2[1].getPath().getName().equals(OBJECT_FILE_NAME));
    assertEquals(1024 * numOfParts, fs2[1].getLen());
    assertTrue(!fs2[1].isDirectory());

    testUtil.compareS3ObjectWithHdfsFile(new FileInputStream(file),
        new Path(s3HdfsPath.getFullHdfsObjPath()));
  }

  @Test
  public void testAbortMultiPartUpload()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {
    // Prepare the object and bucket
    S3Bucket bucket = new S3Bucket("myBucket");

    byte[] data1 = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      data1[i] = (byte) i;
    }
    S3Object object1 = new S3Object("multifile", data1);

    byte[] data2 = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      data2[i] = (byte) i;
    }
    S3Object object2 = new S3Object("multifile", data2);

    MultipartUpload upload =
        s3Service.multipartStartUpload(bucket.getName(), object1.getName(), null);
    MultipartPart part1 = s3Service.multipartUploadPart(upload, 1, object1);
    upload.addMultipartPartToUploadedList(part1);
    MultipartPart part2 = s3Service.multipartUploadPart(upload, 2, object2);
    upload.addMultipartPartToUploadedList(part2);

    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "multifile");
    FileStatus[] fs1 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootUploadPath()));
    assertEquals(2, fs1.length);
    assertTrue(fs1[0].getPath().getName().equals("1" + PART_FILE_NAME));
    assertTrue(fs1[1].getPath().getName().equals("2" + PART_FILE_NAME));
    assertEquals(1024, fs1[0].getLen());
    assertEquals(1024, fs1[1].getLen());

    s3Service.multipartAbortUpload(upload);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath.getHdfsRootVersionPath()));
    assertEquals(1, fs2.length);
    assertTrue(fs2[0].getPath().getName().equals(META_FILE_NAME));
    assertTrue(!fs2[0].isDirectory());
  }

}
