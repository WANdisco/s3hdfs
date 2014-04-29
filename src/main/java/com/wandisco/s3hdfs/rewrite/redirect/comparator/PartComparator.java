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
package com.wandisco.s3hdfs.rewrite.redirect.comparator;

import java.io.Serializable;
import java.util.Comparator;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.PART_FILE_NAME;

public class PartComparator implements Comparator<String>, Serializable {
  private final String path;

  public PartComparator(final String path) {
    this.path = path;
  }

  @Override
  public int compare(final String s1, final String s2) {
    final int part1 =
        Integer.parseInt(s1.replaceFirst(path, "").replace(PART_FILE_NAME, ""));
    final int part2 =
        Integer.parseInt(s2.replaceFirst(path, "").replace(PART_FILE_NAME, ""));
    return (part1 > part2 ? 1 : (part1 == part2 ? 0 : -1));
  }
}
