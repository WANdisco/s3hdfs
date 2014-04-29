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
package com.wandisco.s3hdfs.conf;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration for S3HDFS.
 */
public class S3HdfsConfiguration extends Configuration {
  static {
    // adds the default resources
    Configuration.addDefaultResource("s3hdfs-default.xml");
    Configuration.addDefaultResource("s3hdfs-site.xml");
  }

  public S3HdfsConfiguration(Configuration conf) {
    super(conf);
    init();
  }

  public S3HdfsConfiguration() {
    super();
    init();
  }

  /**
   * This method is here so that when invoked, HdfsConfiguration is class-loaded if
   * it hasn't already been previously loaded.  Upon loading the class, the static
   * initializer block above will be executed to add the deprecated keys and to add
   * the default resources.   It is safe for this method to be called multiple times
   * as the static initializer block will only get invoked once.
   */
  public static void init() {}

  public static void main(String[] args) throws IOException {
    S3HdfsConfiguration s3HdfsConfiguration = new S3HdfsConfiguration();
    s3HdfsConfiguration.writeXml(System.out);
  }
}
