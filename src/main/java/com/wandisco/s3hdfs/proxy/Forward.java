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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_CHARSET;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_HEADER_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_HEADER_VALUE;

/**
 * This is the class for handling the reads and writes of
 * requests and responses. It handles filtering of the Expect header
 * and the addition of the S3HDFS header.
 */
class Forward {
  private static Logger LOG = LoggerFactory.getLogger(S3HdfsProxy.class);

  final ClientThread parent;
  final BufferedReader aInputStream;
  final OutputStream aOutputStream;
  boolean aIgnoreExpect;

  Forward(ClientThread parent, BufferedReader aInputStream,
                OutputStream aOutputStream,
                boolean aIgnoreExpect) {
    this.parent = parent;
    this.aInputStream = aInputStream;
    this.aOutputStream = aOutputStream;
    this.aIgnoreExpect = aIgnoreExpect;
  }

  public void run() {
    try {
      forward();
    } catch (IOException e) {
      parent.setKeepAlive(false);
    }
  }

  private void forward() throws IOException {
    BufferedReader reader = aInputStream;
    OutputStream writer =  aOutputStream;
    boolean printBody = false;
    boolean wroteS3Header = false;
    long contentLength = 0;

    while (true) {
      // STEP 1. Read a line.
      String line = reader.readLine();

      // STEP 2. A null line means a broken connection; terminate.
      if(line == null) {
        parent.setKeepAlive(false);
        parent.getClientSock().setKeepAlive(false);
        parent.getS3HdfsSock().setKeepAlive(false);
        return;
      }

      // STEP 3. Check for Content-Length; if so, we have a body.
      if(line.startsWith("Content-Length: ")) {
        contentLength = Long.parseLong(line.replace("Content-Length: ", ""));
        if (contentLength != 0) {
          printBody = true;
        }
      }
      else if(line.equalsIgnoreCase("Transfer-Encoding: Chunked")) {
        contentLength = -1;
        printBody = true;
      }
      // STEP 4. Check for Connection status; set KeepAlive accordingly.
      else if(line.equalsIgnoreCase("Connection: Keep-Alive")) {
        LOG.debug("Keeping-Alive Connection.");
        parent.setKeepAlive(true);
        parent.getClientSock().setKeepAlive(true);
        parent.getS3HdfsSock().setKeepAlive(true);
      }
      else if(line.equalsIgnoreCase("Connection: Close")) {
        LOG.debug("Closing Connection.");
        parent.setKeepAlive(false);
        parent.getClientSock().setKeepAlive(false);
        parent.getS3HdfsSock().setKeepAlive(false);
      }

      // STEP 5,1. Check for Expect header stripping and write line out.
      if (aIgnoreExpect) {
        if(!line.equalsIgnoreCase("Expect: 100-continue")) {
          LOG.debug("RAW: "+line);
          writer.write((line+"\r\n").getBytes(DEFAULT_CHARSET));

          // STEP 5,2. If we are ignoring Expect and have not injected our
          // special S3HDFS header after the 1st HTTP line, do it now.
          // NOTE: Ignoring Expect means we are the client -> s3Hdfs talk.
          if (!wroteS3Header) {
            LOG.debug("RAW: "+S3_HEADER_NAME+": "+S3_HEADER_VALUE);
            writer.write((S3_HEADER_NAME+": "+S3_HEADER_VALUE+"\r\n")
                .getBytes(DEFAULT_CHARSET));
            wroteS3Header = true;
          }
        } else {
          LOG.debug("Stripping Expect header and skipping body...");
          printBody = false;
        }
      } else {
        // STEP 5,3. If we are not checking Expect header, write line out.
        LOG.debug("RAW: "+line);
        writer.write((line+"\r\n").getBytes(DEFAULT_CHARSET));
      }

      // STEP 6. If line is empty then we are at end of reading headers.
      if (line.isEmpty()) {
        // STEP 7A. If we have a body, content-length, and no Expect header,
        // write body out completely.
        if(printBody && contentLength >= 0) {
          LOG.debug("Started writing body..."+contentLength);
          for(long i = contentLength; i > 0; i--) {
            writer.write(reader.read());
          }
          writer.write("\r\n".getBytes(DEFAULT_CHARSET));
          LOG.debug("Finished writing body...");
        }
        // STEP 7B. If we have a body, chunked encoding, and no Expect header,
        // write chunks out completely and exit after last chunk.
        else if(printBody && contentLength < 0) {
          boolean lastChunk = false;
          while(!lastChunk) {
            int read = reader.read();
            // State 1. We read a 0
            if(read == (int) '0') {
              writer.write(read);
              read = reader.read();
              // State 2. We read a carriage return
              if(read == (int) '\r') {
                writer.write(read);
                read = reader.read();
                // State 3. We read a new line
                if(read == (int) '\n') {
                  writer.write(read);
                  read = reader.read();
                  // State 4. We read another carriage return
                  if(read == (int) '\r') {
                    writer.write(read);
                    read = reader.read();
                    // State 5. We read another new line
                    if(read == (int) '\n') {
                      writer.write(read);
                      lastChunk = true;
                    }
                  }
                }
              }
            }
            writer.write(read);
          }
        }
        // STEP 7C. Write empty line.
        else {
          writer.write("\r\n".getBytes(DEFAULT_CHARSET));
        }
        // STEP 8. Flush out and conclude the transmission.
        writer.flush();
        LOG.debug("Breaking forwardThread...");
        break;
      }
    }
  }
}
