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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

public class S3HdfsFileStatus extends HdfsFileStatus {
  private long objectModTime;
  private long objectSize;
  private String versionId;
  private boolean isDelMarker;
  private boolean isLatest;


  public S3HdfsFileStatus(long length, boolean isdir, int block_replication,
                          long blocksize, long modification_time, long access_time,
                          FsPermission permission, String owner, String group,
                          byte[] symlink, byte[] path, long fileId,
                          int childrenNum) {
    super(length, isdir, block_replication, blocksize, modification_time,
          access_time, permission, owner, group, symlink, path, fileId,
          childrenNum);
  }

  public String getVersionId() {
    return versionId;
  }

  public void setVersionId(String versionId) {
    this.versionId = versionId;
  }

  public long getObjectModTime() {
    return objectModTime;
  }

  public void setObjectModTime(long time) {
    this.objectModTime = time;
  }

  public long getObjectSize() {
    return objectSize;
  }

  public void setObjectSize(long size) {
    this.objectSize = size;
  }

  public boolean isLatest() {
    return isLatest;
  }

  public void setLatest(boolean latest) {
    isLatest = latest;
  }

  public boolean isDelMarker() {
    return isDelMarker;
  }

  public void setDelMarker(boolean delMarker) {
    isDelMarker = delMarker;
  }

}
