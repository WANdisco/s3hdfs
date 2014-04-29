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

import com.wandisco.s3hdfs.path.S3HdfsPath;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3HDFS_COMMAND;

/**
 * Purpose of this wrapper is to change the request as it travels to webHDFS.
 * It converts the S3 query string into webHDFS query string based on the Command.
 */
public class WebHdfsRequestWrapper extends HttpServletRequestWrapper {

  private static Logger LOG = LoggerFactory.getLogger(
      WebHdfsRequestWrapper.class);

  private final HttpServletRequest request;
  private final S3HdfsPath s3HdfsPath;
  private final S3HDFS_COMMAND command;
  private ServletInputStream inputStream;

  public WebHdfsRequestWrapper(final HttpServletRequest hsr,
                               final S3HDFS_COMMAND command,
                               final S3HdfsPath s3HdfsPath) {
    super(hsr);
    this.request = hsr;
    this.command = command;
    this.s3HdfsPath = s3HdfsPath;
  }

  @Override
  public String getQueryString() {
    String userName = s3HdfsPath.getUserName();
    StringBuilder query = userName == null ? new StringBuilder() :
        new StringBuilder(new UserParam(userName).toString());

    //TODO: Move the QueryString logic into modifiedURI in the Commands.

    // build GET logic
    if (request.getMethod().equalsIgnoreCase("GET")) {
      if (command.equals(S3HDFS_COMMAND.GET_OBJECT) ||
          command.equals(S3HDFS_COMMAND.GET_VERSIONING)) {
        query.append("&op=OPEN");
        String rangeHeader = request.getHeader("Range");
        if (rangeHeader != null) {
          long[] ranges = parseRange(rangeHeader);
          long startRange = ranges[0];
          long endRange = ranges[1];
          query.append("&offset=").append(startRange).append("&length=").append(endRange);
        }
      } else
        query.append("&op=LISTSTATUS");
      // build PUT logic
    } else if (request.getMethod().equalsIgnoreCase("PUT")) {
      if (command.equals(S3HDFS_COMMAND.PUT_BUCKET))
        query.append("&op=MKDIRS&permission=777");
      else if (command.equals(S3HDFS_COMMAND.PUT_OBJECT) ||
          command.equals(S3HDFS_COMMAND.UPLOAD_PART) ||
          command.equals(S3HDFS_COMMAND.COPY_OBJECT) ||
          command.equals(S3HDFS_COMMAND.CONFIGURE_VERSIONING))
        query.append("&op=CREATE&overwrite=true");
      // build DELETE logic
    } else if (request.getMethod().equalsIgnoreCase("DELETE")) {
      query.append("&op=DELETE&recursive=true");
      // build POST logic
    } else if (request.getMethod().equalsIgnoreCase("POST")) {
      //query.append("&op=APPEND");
    }
    return query.toString();
  }

  private long[] parseRange(final String rangeHeader) {
    long[] retVal = new long[2];
    String parsed = rangeHeader.replace("bytes=", "").trim();
    String[] longStrs = parsed.split("-");
    // first value from s3 is an offset to start at
    // in HDFS it is also an offset to start at
    retVal[0] = Long.parseLong(longStrs[0]);
    // second value from s3 is the ending byte range
    // in HDFS it is a length to read (end - start)
    long endRange = Long.parseLong(longStrs[1]);
    retVal[1] = endRange - retVal[0];
    return retVal;
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    if (inputStream != null)
      return inputStream;
    return super.getInputStream();
  }

  public void setInputStream(ServletInputStream is) {
    inputStream = is;
  }

  @Override
  public String getParameter(final String name) {
    if (name.equals(UserParam.NAME))
      return s3HdfsPath.getUserName();
    return super.getParameter(name);
  }
}
