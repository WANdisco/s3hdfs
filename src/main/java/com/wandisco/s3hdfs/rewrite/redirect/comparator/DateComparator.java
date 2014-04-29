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

import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsFileStatus;

import java.io.Serializable;
import java.util.Comparator;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_VERSION;

public class DateComparator implements Comparator<S3HdfsFileStatus>, Serializable {

  @Override
  public int compare(S3HdfsFileStatus thisStat, S3HdfsFileStatus thatStat) {
    final long thisModTime = (thisStat.getVersionId().equals(DEFAULT_VERSION)) ?
        Long.MIN_VALUE : thisStat.getModificationTime();
    final long thatModTime = (thatStat.getVersionId().equals(DEFAULT_VERSION)) ?
        Long.MIN_VALUE : thatStat.getModificationTime();
    return (thisModTime > thatModTime ? 1 :
        (thisModTime == thatModTime ? 0 : -1));
  }

}
