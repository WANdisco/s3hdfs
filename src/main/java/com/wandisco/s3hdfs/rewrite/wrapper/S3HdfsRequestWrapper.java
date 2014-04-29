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

import java.io.IOException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * An HttpServletResponseWrapper designed to capture the response status
 * code of a Response as it travels through the Servlet.
 */
public class S3HdfsRequestWrapper extends HttpServletRequestWrapper {

  private ServletInputStream inputStream;

  /**
   * Constructs a response adaptor wrapping the given response.
   *
   * @throws IllegalArgumentException if the response is null
   */
  public S3HdfsRequestWrapper(HttpServletRequest request) {
    super(request);
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    if(inputStream != null)
      return inputStream;
    return getRequest().getInputStream();
  }

  public void setInputStream(ServletInputStream is) {
    inputStream = is;
  }

  @Override
  public String toString() {
    return getRequest().toString();
  }
}
