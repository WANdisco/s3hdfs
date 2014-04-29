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
package com.wandisco.s3hdfs.proxy;

import com.wandisco.s3hdfs.conf.S3HdfsConfiguration;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_MAX_CONNECTIONS_KEY;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_MAX_CONNECTIONS_DEFAULT;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_SERVICE_HOSTNAME_KEY;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_SERVICE_HOSTNAME_DEFAULT;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_PROXY_PORT_KEY;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_PROXY_PORT_DEFAULT;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY;

/**
 * This is a Singleton proxy server intended to redirect local traffic from
 * a configured s3Port to the webHDFS s3Port of the Hadoop Cluster it is a
 * part of.
 */
public class S3HdfsProxy extends Thread {
  private static Logger LOG = LoggerFactory.getLogger(S3HdfsProxy.class);
  private static S3HdfsProxy instance;

  private final String serviceHost;
  private final String nnPort;
  private final int s3Port;
  private final int maxConnections;

  /**
   * Returns a running instance of the S3HdfsProxy service.
   * @param port the s3Port to run the proxy on
   * @param nnPort the namenode http address
   * @param serviceHost the service host name
   */
  public static synchronized void startInstance(int port, String nnPort,
                                                String serviceHost, int max) {
    if (instance == null) {
      instance = new S3HdfsProxy(port, nnPort, serviceHost, max);
      instance.start();
    }
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = new HdfsConfiguration(new S3HdfsConfiguration());
    startInstance(
        Integer.decode(conf.get(S3_PROXY_PORT_KEY, S3_PROXY_PORT_DEFAULT)),
        conf.get(DFS_NAMENODE_HTTP_PORT_KEY, String.valueOf(DFS_NAMENODE_HTTP_PORT_DEFAULT)),
        conf.get(S3_SERVICE_HOSTNAME_KEY, S3_SERVICE_HOSTNAME_DEFAULT),
        Integer.decode((conf.get(S3_MAX_CONNECTIONS_KEY, S3_MAX_CONNECTIONS_DEFAULT)))
    );
  }

  public S3HdfsProxy(int port, String nnHost, String serviceHost, int maxConnections) {
    this.s3Port = port;
    this.nnPort = nnHost;
    this.serviceHost = serviceHost;
    this.maxConnections = maxConnections;
  }

  public void run() {
    LOG.info("Starting S3HdfsProxy...");
    ServerSocket listener = null;
    boolean run = true;

    try {
      listener = new ServerSocket(s3Port);
    } catch (IOException e) {
      e.printStackTrace();
      run = false;
    }

    ExecutorService pool = Executors.newFixedThreadPool(maxConnections);
    while (run) {
      try {
        if(!listener.isClosed()) {
          Socket clientSocket = listener.accept();
          pool.submit(new ClientThread(nnPort, serviceHost, clientSocket));
          LOG.debug("Submitted a new ClientThread.");
        } else {
          run = false;
        }
      } catch (Exception e) {
        e.printStackTrace();
      } catch (Error e) {
        e.printStackTrace();
        run = false;
      }
    }
    pool.shutdownNow();
    LOG.info("Stopping S3HdfsProxy...");
  }

}