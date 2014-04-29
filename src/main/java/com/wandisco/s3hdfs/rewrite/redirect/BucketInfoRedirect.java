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

import com.wandisco.s3hdfs.path.S3HdfsPath;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsResponseWrapper;
import org.apache.commons.httpclient.methods.GetMethod;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.HTTP_METHOD.GET;

public class BucketInfoRedirect extends Redirect {
  public BucketInfoRedirect(S3HdfsRequestWrapper request,
                            S3HdfsResponseWrapper response,
                            S3HdfsPath s3HdfsPath) {
    super(request, response, s3HdfsPath);
  }

  public boolean checkEmpty() throws IOException {
    GetMethod httpGet = (GetMethod) getHttpMethod(request.getScheme(),
        request.getServerName(), request.getServerPort(), "GETCONTENTSUMMARY",
        path.getUserName(), path.getHdfsRootBucketPath(), GET);
    httpClient.executeMethod(httpGet);
    String contentSummaryJson =
        readInputStream(httpGet.getResponseBodyAsStream());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonRoot = mapper.readTree(contentSummaryJson);
    int dirs = jsonRoot.get("ContentSummary").get("directoryCount").getIntValue();
    return (dirs == 1);
  }
}
