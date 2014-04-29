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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_CHARSET;

/**
 * Inner class intended to perform a single request / response transaction
 * between a client and S3HDFS.
 *
 * Special features: strips any Expect: 100-continue headers
 * and adds a special S3HDFS header to signal processing requests.
 */
class ClientThread implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(S3HdfsProxy.class);
  private final String serviceHost;
  private final String nnPort;

  private volatile boolean keepAlive = true;
  private final Socket clientSock;
  private Socket s3HdfsSock;

  public ClientThread(String nnPort, String serviceHost, Socket clientSock)
      throws SocketException {
    this.nnPort = nnPort;
    this.serviceHost = serviceHost;
    this.clientSock = clientSock;
  }

  @Override
  public void run() {
    BufferedReader clientInputStream = null;
    BufferedReader s3HdfsInputStream = null;
    OutputStream clientOutputStream = null;
    OutputStream s3HdfsOutputStream = null;

    do {
      try {
        for(int i = 0; i < 10 && s3HdfsSock == null; i++) {
          String rawAddress = serviceHost + ":" + nnPort;
          try {
            s3HdfsSock = new Socket(serviceHost, Integer.decode(nnPort));
          } catch (ConnectException e) {
            LOG.error("["+(i+1)+"] Connection refused... retrying: "+rawAddress);
          } catch (SocketException e) {
            LOG.error("["+(i+1)+"] Connection reset... closing.");
            return;
          }
        }

        if(s3HdfsSock == null) {
          LOG.info("Connection failed.");
          return;
        }

        clientInputStream = new BufferedReader(new InputStreamReader(
            clientSock.getInputStream(), DEFAULT_CHARSET));
        s3HdfsInputStream = new BufferedReader(new InputStreamReader(
            s3HdfsSock.getInputStream(), DEFAULT_CHARSET));
        clientOutputStream = clientSock.getOutputStream();
        s3HdfsOutputStream = s3HdfsSock.getOutputStream();
      } catch (IOException e) {
        LOG.error("Failed to set up sockets.", e);
        break;
      }

      try {
        LOG.debug("Calling doProxy()...");
        doProxy(clientInputStream, s3HdfsInputStream,
            clientOutputStream, s3HdfsOutputStream);
        s3HdfsSock.close();
        s3HdfsSock = null;
      } catch (IOException e) {
        LOG.error("Failed to finish doProxy().", e);
        break;
      }

    } while(keepAlive);

    try {
      if(clientInputStream != null) clientInputStream.close();
      if(s3HdfsInputStream != null) s3HdfsInputStream.close();
      if(clientOutputStream != null) clientOutputStream.close();
      if(s3HdfsOutputStream != null) s3HdfsOutputStream.close();
      clientSock.close();
    } catch (IOException e) {
      LOG.error("Failed to close sockets.", e);
    } finally {
      LOG.debug("Exiting ClientThread...");
    }
  }

  public Socket getClientSock() {
    return clientSock;
  }

  public Socket getS3HdfsSock() {
    return s3HdfsSock;
  }

  public void setKeepAlive(boolean keepAlive) {
    this.keepAlive = keepAlive;
  }

  /**
   * Setup the threads and perform communication.
   */
  private void doProxy(BufferedReader clientIn, BufferedReader s3In,
                       OutputStream clientOut, OutputStream s3Out)
      throws IOException {

    // LISTEN TO CLIENT, SPEAK TO S3HDFS
    Forward client2s3hdfs =
        new Forward(this, clientIn, s3Out, true);
    client2s3hdfs.run();

    if(!keepAlive)
      return;

    LOG.debug("SENT A REQUEST.");

    //LISTEN TO S3HDFS, SPEAK TO CLIENT
    Forward s3hdfs2client =
        new Forward(this, s3In, clientOut, false);
    s3hdfs2client.run();

    LOG.debug("GOT A RESPONSE.");

  }
}
