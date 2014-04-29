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
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.junit.Test;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.OBJECT_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCopy extends TestBase {

  @Test
  public void testBasicCopy()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("folder", "file1");
    S3HdfsPath s3HdfsPath2 = testUtil.setUpS3HdfsPath("folder", "file2");

    // Get the object
    S3Bucket bucket = new S3Bucket(s3HdfsPath1.getBucketName());

    // Put new object
    byte[] data = "my kingdom for a donkey".getBytes();
    S3Object object1 = new S3Object(s3HdfsPath1.getObjectName(), data);
    s3Service.putObject(bucket.getName(), object1);

    // Prepare copy object
    S3Object object2 = new S3Object(s3HdfsPath2.getObjectName(), "");

    s3Service.copyObject(bucket.getName(), object1.getKey(),
        bucket.getName(), object2, false);

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath2.getFullHdfsObjPath()));
    assertEquals(1, fs2.length);
    assertTrue(fs2[0].getPath().getName().equals(OBJECT_FILE_NAME));
    assertTrue(!fs2[0].isDirectory());
    assertEquals(data.length, fs2[0].getLen());
  }

  @Test
  public void testBigCopy()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("folder", "file1");
    S3HdfsPath s3HdfsPath2 = testUtil.setUpS3HdfsPath("folder", "file2");

    // Get the object
    S3Bucket bucket = new S3Bucket(s3HdfsPath1.getBucketName());

    // Put new object
    File file = testUtil.getFile(BIG_SIZE);

    S3Object object1 = new S3Object(s3HdfsPath1.getObjectName());
    object1.setDataInputFile(file);
    s3Service.putObject(bucket.getName(), object1);

    // Prepare copy object
    S3Object object2 = new S3Object(s3HdfsPath2.getObjectName(), "");

    s3Service.copyObject(bucket.getName(), object1.getKey(),
        bucket.getName(), object2, false);

    FileStatus[] fs2 =
        hdfs.listStatus(new Path(s3HdfsPath2.getFullHdfsObjPath()));
    assertEquals(1, fs2.length);
    assertTrue(fs2[0].getPath().getName().equals(OBJECT_FILE_NAME));
    assertTrue(!fs2[0].isDirectory());
    assertEquals(BIG_SIZE, fs2[0].getLen());

    testUtil.compareS3ObjectWithHdfsFile(new FileInputStream(file),
        new Path(s3HdfsPath2.getFullHdfsObjPath()));
  }

  @Test
  public void testBasicRename()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException {
    S3HdfsPath s3HdfsPath1 = testUtil.setUpS3HdfsPath("folder", "file1");
    S3HdfsPath s3HdfsPath2 = testUtil.setUpS3HdfsPath("folder", "file2");

    // Get the object
    S3Bucket bucket = new S3Bucket(s3HdfsPath1.getBucketName());

    // Put new object
    File file = testUtil.getFile(BIG_SIZE);

    S3Object object1 = new S3Object(s3HdfsPath1.getObjectName());
    S3Object object2 = new S3Object(s3HdfsPath2.getObjectName());
    object1.setDataInputFile(file);
    s3Service.putObject(bucket.getName(), object1);

    // Rename is done as a PUT, COPY, and DELETE
    s3Service.renameObject(bucket.getName(), object1.getKey(), object2);

    // Compare rename with original
    testUtil.compareS3ObjectWithHdfsFile(new FileInputStream(file),
        new Path(s3HdfsPath2.getFullHdfsObjPath()));
  }

}
