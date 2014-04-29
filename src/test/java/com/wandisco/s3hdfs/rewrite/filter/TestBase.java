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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.S3Service;
import org.junit.After;
import org.junit.Before;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.Assert.assertTrue;

public class TestBase {
  public static int SMALL_SIZE = 12345;
  public static int BIG_SIZE = 500000;
  public static int HTTP_PORT = 50070;
  public static int PROXY_PORT = 50005;
  S3HdfsTestUtil testUtil;
  S3Service s3Service;
  MiniDFSCluster cluster;
  DistributedFileSystem hdfs;
  String s3Directory;
  String hostName;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration(new S3HdfsConfiguration());
    conf.setInt(S3_PROXY_PORT_KEY, PROXY_PORT);
    conf.setBoolean(DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setInt(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, 100);
    conf.setLong(DFS_BLOCK_SIZE_KEY, 1024);
    conf.setLong(DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 512);

    // ^ has to be a multiple of 512
    FsPermission.setUMask(conf, FsPermission.createImmutable((short) 0));
    // ^ eliminate the UMask in HDFS to remove perm denied exceptions in s3Dir
    hostName = conf.get(S3_SERVICE_HOSTNAME_KEY);
    System.out.println("S3HDFS ServiceHostName: " + hostName);

    s3Directory = conf.get(S3_DIRECTORY_KEY);
    cluster = new MiniDFSCluster.Builder(conf).nameNodeHttpPort(HTTP_PORT)
        .numDataNodes(3).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();

    //initialize s3 directory
    Path s3Path = new Path(s3Directory);
    assertTrue(hdfs.mkdirs(s3Path));

    testUtil = new S3HdfsTestUtil(hdfs, s3Directory);
    s3Service = testUtil.configureS3Service(hostName, PROXY_PORT);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    if (s3Service != null) {
      s3Service.shutdown();
      s3Service = null;
    }
    if (hdfs != null) {
      hdfs.delete(new Path(s3Directory), true);
      hdfs.close();
      hdfs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
