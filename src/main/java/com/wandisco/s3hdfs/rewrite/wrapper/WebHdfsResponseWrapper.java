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
package com.wandisco.s3hdfs.rewrite.wrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 * Purpose of this wrapper is to fetch the content body from webHDFS.
 */
public class WebHdfsResponseWrapper extends HttpServletResponseWrapper {

  private static Logger LOG = LoggerFactory.getLogger(
                                            WebHdfsResponseWrapper.class);

  protected OutputStream realOutputStream = null;
  protected ServletOutputStream stream = null;
  protected PrintWriter writer = null;

  public WebHdfsResponseWrapper(final HttpServletResponse response) {
    super(response);
  }
  
  public ServletOutputStream createOutputStream() throws IOException {
    try {
      realOutputStream = new ByteArrayOutputStream();
      return new ResponseStreamWrapper(realOutputStream);
    }
    catch (Exception ex) {
      LOG.error("Could not create OutputStream class.", ex);
    }
    return null;
  }
 
  @Override
  public void flushBuffer() throws IOException {
    stream.flush();
  }
 
  @Override
  public ServletOutputStream getOutputStream() throws IOException {
    if (writer != null) {
      throw new IllegalStateException("getOutputStream() has already been called!");
    }
 
    if (stream == null) {
      stream = createOutputStream();
    }
    return stream;
  }
 
  @Override
  public PrintWriter getWriter() throws IOException {
    if (writer != null) {
      return (writer);
    }
 
    if (stream != null) {
      throw new IllegalStateException("getOutputStream() has already been called!");
    }
 
   stream = createOutputStream();
   writer = new PrintWriter(new OutputStreamWriter(stream, "UTF-8"));
   return (writer);
  }

  @Override
  public void setContentLength(int length) {
    super.setContentLength(length);
  }

  /**
   * Gets the underlying content of the output stream.
   * @return content as a String
   */
  public String getContent() {
    return realOutputStream.toString();
  }

  /**
   * Clears the output stream so we can record again.
   */
  public void clearOutputStream() {
    stream = null;
    writer = null;
    realOutputStream = null;
  }

  @Override
  public String toString() {
    return getResponse().toString();
  }

}
