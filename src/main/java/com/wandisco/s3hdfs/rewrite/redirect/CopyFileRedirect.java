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
package com.wandisco.s3hdfs.rewrite.redirect;

import com.wandisco.s3hdfs.rewrite.wrapper.RequestStreamWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import org.apache.commons.httpclient.methods.GetMethod;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.GET;

public class CopyFileRedirect extends Redirect {

  public CopyFileRedirect(HttpServletRequest request,
                          HttpServletResponse response) {
    super(request, response, null);
    LOG.debug("Created " + getClass().getSimpleName() + ".");
  }

  /**
   * Sends a PUT command to create the container directory inside of HDFS.
   * It uses the URL from the original request to do so.
   *
   * @param nameNodeHttpHost
   * @param userName
   * @throws IOException
   * @throws ServletException
   */
  public void sendCopy(String nameNodeHttpHost, String userName,
                       String srcBucket, String srcObject)
      throws IOException, ServletException {
    // Set up HttpGet and get original file
    String uri = replaceSrcs(request.getRequestURI(), srcBucket, srcObject);
    String[] nnHost = nameNodeHttpHost.split(":");

    GetMethod httpGet =
        (GetMethod) getHttpMethod(request.getScheme(), nnHost[0],
            Integer.decode(nnHost[1]), "OPEN", userName, uri, GET);

    // Try up to 5 times to get the source file
    httpClient.executeMethod(httpGet);
    LOG.debug("1st response: " + httpGet.getStatusLine().toString());

    for (int i = 0; i < 5 && httpGet.getStatusCode() == 403; i++) {
      httpGet.releaseConnection();
      httpClient.executeMethod(httpGet);
      LOG.debug("Next response: " + httpGet.getStatusLine().toString());
    }

    assert httpGet.getStatusCode() == 200;
    assert request instanceof S3HdfsRequestWrapper;
    ((S3HdfsRequestWrapper) request).setInputStream(
        new RequestStreamWrapper(httpGet.getResponseBodyAsStream()));
  }

  public void completeCopy() throws IOException {
    SimpleDateFormat rc228 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz");
    String modTime = rc228.format(Calendar.getInstance().getTime());
    response.setContentType("application/xml");
    response.getOutputStream().write(("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<CopyObjectResult>" + "<LastModified>" + modTime + "</LastModified>" +
        "<ETag>HARDCODED1234567890</ETag>" + "</CopyObjectResult>").getBytes("UTF-8"));
  }

}
