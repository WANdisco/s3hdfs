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

import com.wandisco.s3hdfs.conf.S3HdfsConfiguration;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_PROXY_PORT_DEFAULT;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_DIRECTORY_KEY;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_PROXY_PORT_KEY;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_SERVICE_HOSTNAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestConcurrency extends TestBase {

  public static final String masterBucket = "masterBucket";
  public static final String masterObjectName = "masterObject";

  void prepMasterBucket() throws IOException, ServiceException {
    // Configure the service
    s3Service.createBucket(masterBucket);
    assertEquals(masterBucket, s3Service.getBucket(masterBucket).getName());
  }

  class ConcurrentClient implements Runnable
  {
    int id = -1;
    boolean useMasterObject = false;
    boolean passed = false;

    ConcurrentClient(int id, boolean useMasterObject) {
      this.id = id;
      this.useMasterObject = useMasterObject;
    }

    void makeObjects()
        throws IOException, URISyntaxException, ServiceException, NoSuchAlgorithmException {
      // Configure the service
      S3Service s3Service = testUtil.configureS3Service(hostName, PROXY_PORT);

      // Prepare objects inside MasterBucket; PUT and READ
      int randInt = Math.abs(new Random().nextInt() % 10) + 2;
      for(int i = 0; i < randInt; i++) {
        String oid = (useMasterObject) ? masterObjectName :
                                         "object_" + id + "_" + i;
        byte[] data = new byte[SMALL_SIZE];
        for(int d = 0; d < SMALL_SIZE; d++) {
          data[i] = (byte) (d % 256);
        }
        S3Object s3Object = new S3Object(oid, data);
        s3Service.putObject(masterBucket, s3Object);
        S3Object object =
          s3Service.getObject(masterBucket, s3Object.getKey());
        InputStream dataInputStream = object.getDataInputStream();
        for(int j = 0; j < SMALL_SIZE; j++) {
          int read = dataInputStream.read();
          if(read == -1) fail();
        }
        assertEquals(-1, dataInputStream.read());
        dataInputStream.close();
      }

      // Shutdown the service
      s3Service.shutdown();
      passed = true;
    }

    public void run() {
      try {
        this.makeObjects();
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  private void runConcurrentClients (int clientsToRun, boolean sameObject) {
    Thread[] threads = new Thread[clientsToRun];
    ConcurrentClient[] clients = new ConcurrentClient[clientsToRun];
    for (int i = 0; i < clientsToRun; i++) {
      ConcurrentClient client = new ConcurrentClient(i+1, sameObject);
      Thread thread = new Thread(client);
      thread.start();
      threads[i] = thread;
      clients[i] = client;
    }
    for (int i = 0; i < clientsToRun; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail();
      }
      assertTrue(clients[i].passed);
      assertTrue(!threads[i].isAlive());
    }
  }

  @Test
  public void testFiveRandom() throws IOException, ServiceException {
    prepMasterBucket();
    runConcurrentClients(5, false);
  }

  //@Test //PJJ: Not testing...
  public void testFiftyRandom() throws IOException, ServiceException {
    prepMasterBucket();
    runConcurrentClients(50, false);
  }

  //@Test //PJJ: Not testing...
  public void testFiveWithConcurrentWrite() throws IOException, ServiceException {
    prepMasterBucket();
    runConcurrentClients(5, true);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf =
        new HdfsConfiguration(new S3HdfsConfiguration());
    DistributedFileSystem hdfs =
        (DistributedFileSystem) DistributedFileSystem.get(conf);

    PROXY_PORT =
        Integer.decode(conf.get(S3_PROXY_PORT_KEY, S3_PROXY_PORT_DEFAULT));

    TestConcurrency test = new TestConcurrency();
    test.hdfs = hdfs;
    test.s3Directory = conf.get(S3_DIRECTORY_KEY);
    test.hostName = conf.get(S3_SERVICE_HOSTNAME_KEY);
    test.testUtil = new S3HdfsTestUtil(test.hdfs, test.s3Directory);
    test.s3Service = test.testUtil.configureS3Service(test.hostName, PROXY_PORT);
    test.testFiveRandom();

    hdfs.close();
  }
}
