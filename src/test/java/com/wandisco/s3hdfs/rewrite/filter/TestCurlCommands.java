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
import com.wandisco.s3hdfs.rewrite.xml.S3XmlWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCurlCommands extends TestBase {

  @Test
  public void testCurlCreateBucket1()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("rewrite", null, "flavaflav");

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "http://" + hostName + ":" + PROXY_PORT + "/rewrite?user.name=flavaflav");
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.err.println(out);
    System.err.println(out2);

    FileStatus retVal = hdfs.listStatus(new Path(
        s3HdfsPath.getHdfsRootUserPath()))[0];
    System.out.println(
        hdfs.listStatus(new Path(
            s3HdfsPath.getHdfsRootUserPath()))[0].getPath()
    );

    assertEquals("rewrite", retVal.getPath().getName());
    assertEquals("flavaflav", retVal.getOwner());
    assertTrue(retVal.isDirectory());

    FileStatus[] inside = hdfs.listStatus(new Path(retVal.getPath().toString()));
    assertEquals(0, inside.length);
  }

  @Test
  public void testCurlCreateBucket2()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("rewrite", null, "flavaflav");

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "-H", "Host: rewrite." + hostName,
        "http://" + hostName + ":" + PROXY_PORT + "/?user.name=flavaflav");
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    FileStatus retVal = hdfs.listStatus(new Path(
        s3HdfsPath.getHdfsRootUserPath()))[0];
    System.out.println(
        hdfs.listStatus(new Path(
            s3HdfsPath.getHdfsRootUserPath()))[0].getPath()
    );

    assertEquals("rewrite", retVal.getPath().getName());
    assertEquals("flavaflav", retVal.getOwner());
    assertTrue(retVal.isDirectory());

    FileStatus[] inside = hdfs.listStatus(new Path(retVal.getPath().toString()));
    assertEquals(0, inside.length);
  }

  @Test
  public void testCurlCreateObject1()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    S3HdfsPath s3HdfsPath =
        testUtil.setUpS3HdfsPath("myBucket", "object", "flavaflav");

    File file = testUtil.getFile(SMALL_SIZE);

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "-T", file.getAbsolutePath(), "http://" + hostName + ":" + PROXY_PORT +
        "/myBucket/object?user.name=flavaflav"
    );
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    DataInputStream dataInputStream =
        hdfs.open(new Path(s3HdfsPath.getFullHdfsObjPath()));
    String hdfsData = testUtil.readInputStream(dataInputStream);
    String rawData = testUtil.readInputStream(new FileInputStream(file));

    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getLen());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getLen());

    assertEquals(rawData, hdfsData);
  }

  @Test
  public void testCurlCreateObject2()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "object", "flavaflav");

    File file = testUtil.getFile(SMALL_SIZE);

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "-H", "Host: myBucket." + hostName, "-T", file.getAbsolutePath(),
        "http://" + hostName + ":" + PROXY_PORT + "/object?user.name=flavaflav");
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    DataInputStream dataInputStream =
        hdfs.open(new Path(s3HdfsPath.getFullHdfsObjPath()));
    String hdfsData = testUtil.readInputStream(dataInputStream);
    String rawData = testUtil.readInputStream(new FileInputStream(file));

    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getLen());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getLen());

    assertEquals(rawData, hdfsData);
  }

  //@Test //PJJ: this test does not work yet because DNS buckets disabled.
  public void testCurlCreateObject3()
      throws IOException, URISyntaxException, S3ServiceException,
      InterruptedException {
    S3HdfsPath s3HdfsPath =
        testUtil.setUpS3HdfsPath("myBucket", "object", "flavaflav");

    File file = testUtil.getFile(SMALL_SIZE);

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT", "-T",
        file.getAbsolutePath(), "http://myBucket." + hostName + ":" + PROXY_PORT +
        "/object?user.name=flavaflav"
    );
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    DataInputStream dataInputStream =
        hdfs.open(new Path(s3HdfsPath.getFullHdfsObjPath()));
    String hdfsData = testUtil.readInputStream(dataInputStream);
    String rawData = testUtil.readInputStream(new FileInputStream(file));

    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getLen());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getLen());

    assertEquals(rawData, hdfsData);
  }

  @Test
  public void testCurlCreateObject4()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket",
        "S3HDFS%2Fslot%2D01special%2Dtapestart", "flavaflav");

    File file = testUtil.getFile(SMALL_SIZE);

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "-T", file.getAbsolutePath(), "http://" + hostName + ":" + PROXY_PORT +
        "/myBucket/" + s3HdfsPath.getObjectName() + "?user.name=flavaflav"
    );
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    DataInputStream dataInputStream =
        hdfs.open(new Path(s3HdfsPath.getFullHdfsObjPath()));
    String hdfsData = testUtil.readInputStream(dataInputStream);
    String rawData = testUtil.readInputStream(new FileInputStream(file));

    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsObjPath()))[0].getLen());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getPath());
    System.out.println(
        hdfs.listStatus(new Path(s3HdfsPath.getFullHdfsMetaPath()))[0].getLen());

    assertEquals("S3HDFS/slot-01special-tapestart", s3HdfsPath.getObjectName());
    assertEquals(rawData, hdfsData);
  }

  @Test
  public void testCurlGetNonExistantObject()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    // WITHOUT BUCKET
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket",
        "S3HDFS%2Fslot%2D01special%2Dtapestart", "flavaflav");

    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "GET",
        "http://" + hostName + ":" + PROXY_PORT + "/myBucket/" + s3HdfsPath.getObjectName() +
            "?user.name=flavaflav"
    );
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    assert out2.contains("HTTP/1.1 404 Not Found");

    // MAKE BUCKET
    ProcessBuilder pb2 = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "-H", "Host: myBucket." + hostName,
        "http://" + hostName + ":" + PROXY_PORT + "/?user.name=flavaflav");
    Process proc2 = pb2.start();
    proc2.waitFor();

    out = testUtil.readInputStream(proc2.getInputStream());
    out2 = testUtil.readInputStream(proc2.getErrorStream());
    System.out.println(out);
    System.out.println(out2);

    FileStatus retVal = hdfs.listStatus(new Path(
        s3HdfsPath.getHdfsRootUserPath()))[0];
    System.out.println(
        hdfs.listStatus(new Path(
            s3HdfsPath.getHdfsRootUserPath()))[0].getPath()
    );

    assertEquals("myBucket", retVal.getPath().getName());
    assertEquals("flavaflav", retVal.getOwner());
    assertTrue(retVal.isDirectory());

    FileStatus[] inside = hdfs.listStatus(new Path(retVal.getPath().toString()));
    assertEquals(0, inside.length);

    //WITH BUCKET
    ProcessBuilder pb3 = new ProcessBuilder("curl", "-v", "-L", "-X", "GET",
        "http://" + hostName + ":" + PROXY_PORT + "/myBucket/" + s3HdfsPath.getObjectName() +
            "?user.name=flavaflav"
    );
    Process proc3 = pb3.start();
    proc3.waitFor();

    out = testUtil.readInputStream(proc3.getInputStream());
    out2 = testUtil.readInputStream(proc3.getErrorStream());
    System.out.println("LAST: " + out);
    System.out.println("LAST: " + out2);

    assert out2.contains("HTTP/1.1 404 Not Found");
  }

  @Test
  public void testCurlBasicMetadataCreate()
      throws IOException, InterruptedException, ServiceException {
    S3HdfsPath s3HdfsPath = testUtil.setUpS3HdfsPath("myBucket", "bigFile");

    // Put new object
    File file = testUtil.getFile(SMALL_SIZE);

    //BIG PUT
    ProcessBuilder pb = new ProcessBuilder("curl", "-v", "-L", "-X", "PUT",
        "http://" + hostName + ":" + PROXY_PORT + "/myBucket/bigFile", "-H",
        "x-amz-meta-scared: yes", "-H", "x-amz-meta-tired: yes", "-H",
        "x-amz-meta-hopeless: never", "-T", file.getAbsolutePath());
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    String out2 = testUtil.readInputStream(proc.getErrorStream());
    System.err.println(out);
    System.err.println(out2);

    assertTrue(testUtil.checkMetadataCreated(s3HdfsPath));
    assertTrue(testUtil.checkMetadataEntries(s3HdfsPath));

    //GET
    ProcessBuilder pb2 = new ProcessBuilder("curl", "-v", "-L", "-X", "GET",
        "http://" + hostName + ":" + PROXY_PORT + "/myBucket/bigFile", "-H",
        "Authorization: AWS plamenjeliazkov:cIY4d2dlrFQmUFqMEMZHYH2JLz4=", "-H",
        "Host: " + hostName + ":" + PROXY_PORT, "-H", "Date: Thu, 17 Jan 2013 01:06:24 GMT",
        "-H", "Accept:", "-H", "User-Agent:");
    Process proc2 = pb2.start();
    proc2.waitFor();

    out = testUtil.readInputStream(proc2.getInputStream());
    out2 = testUtil.readInputStream(proc2.getErrorStream());
    System.err.println(out);
    System.err.println(out2);

    testUtil.compareS3ObjectWithHdfsFile(new FileInputStream(file),
        new Path(s3HdfsPath.getFullHdfsObjPath()));
  }

  @Test
  public void testCurlListObjects()
      throws IOException, URISyntaxException,
      ServiceException, NoSuchAlgorithmException, InterruptedException {

    // Configure the service
    S3Service s3Service = testUtil.configureS3Service(hostName, PROXY_PORT);

    String bucketName = "myBucket";
    File file = testUtil.getFile(SMALL_SIZE);

    // Prepare buckets
    int randInt = Math.abs(new Random().nextInt() % 10) + 1;
    for (int i = 0; i < randInt; i++) {
      Process p = Runtime.getRuntime().exec(
          "curl -v -L -X PUT -T " + file.getAbsolutePath() +
              " http://" + hostName + ":" + PROXY_PORT + "/" +
              bucketName + "/object" + i
      );
      p.waitFor();
    }

    // List objects
    S3Object[] retObjects = s3Service.listObjects(bucketName);
    assert retObjects.length == randInt :
        "Objects created: " + randInt + ", objects returned: " +
            Arrays.toString(retObjects);
    for (S3Object retObject : retObjects) {
      System.out.println(retObject);
    }
  }

  @Test
  public void testCurlEnabledCheck()
      throws IOException, URISyntaxException, S3ServiceException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder("curl", "-v",
        "http://" + hostName + ":" + PROXY_PORT + "/s3hdfs/check");
    Process proc = pb.start();
    proc.waitFor();

    String out = testUtil.readInputStream(proc.getInputStream());
    System.err.println(out);

    assertTrue(out.equals(S3XmlWriter.writeCheckToXml()));
  }

}
