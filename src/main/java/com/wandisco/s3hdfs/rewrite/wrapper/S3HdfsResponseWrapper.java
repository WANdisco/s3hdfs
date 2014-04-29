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

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;

/**
 * An HttpServletResponseWrapper designed to capture the response status
 * code of a Response as it travels through the Servlet.
 */
public class S3HdfsResponseWrapper extends HttpServletResponseWrapper {

  private int status = 200; // default status

  /**
   * Constructs a response adaptor wrapping the given response.
   *
   * @throws IllegalArgumentException if the response is null
   */
  public S3HdfsResponseWrapper(HttpServletResponse response) {
    super(response);
  }

  @Override
  public void sendError(int sc) throws IOException {
    this.status = sc;
    super.sendError(sc);
  }

  @Override
  public void sendError(int sc, String msg) throws IOException {
    this.status = sc;
    super.sendError(sc, msg);
  }

  @Override
  public void sendRedirect(String location) throws IOException {
    this.status = 302;
    super.sendRedirect(location);
  }

  public int getStatus() {
    return this.status;
  }

  @Override
  public void setStatus(int sc) {
    this.status = sc;
    super.setStatus(sc);
  }

  @Override
  public String toString() {
    return getResponse().toString();
  }
}
