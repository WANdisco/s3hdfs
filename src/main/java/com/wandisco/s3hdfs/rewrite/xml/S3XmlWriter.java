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
package com.wandisco.s3hdfs.rewrite.xml;

import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsFileStatus;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.PART_FILE_NAME;
import static com.wandisco.s3hdfs.conf.S3HdfsConstants.S3_VERSION;

/**
 * S3XmlWriter
 */
public class S3XmlWriter {

  private final StringBuilder finalOutput = new StringBuilder();
  private final String userName;
  private final JsonNode origJSON;

  public S3XmlWriter(final String content, final String userName) {
    JsonNode jsonRoot;
    ObjectMapper mapper = new ObjectMapper();
    if(content != null)
      try {
        jsonRoot = mapper.readTree(content);
      } catch (IOException e) {
        jsonRoot = null;
      }
    else
      jsonRoot = null;
    this.origJSON = jsonRoot;
    this.userName = userName;
  }

  public static String writeCheckToXml() {
    StringBuilder builder = new StringBuilder();
    builder.append("<s3hdfs><enabled/><version>");
    builder.append(S3_VERSION);
    builder.append("</version></s3hdfs>");
    return builder.toString();
  }

  public String writeMultiPartInitiateToXml(final String bucketName,
                                            final String objectKey) {
    //writes once
    writeXMLHeader();
    //writes once
    writeInitiateMultiPartHead();
    //writes once
    writeBucket(bucketName);
    //writes once
    writeObject(objectKey);
    //writes once
    writeUploadId();
    //writes once
    writeInitiateMultiPartTail();
    return finalOutput.toString();
  }

  public String writeNoneBucketsToXml() {
    //writes once
    writeXMLHeader();
    //writes once
    writeAllMyBucketsHead();
    //writes once
    writeOwner();
    // open buckets bracket
    finalOutput.append("<Buckets>");
    // close buckets bracket
    finalOutput.append("</Buckets>");
    //writes once
    writeAllMyBucketsTail();
    return finalOutput.toString();
  }

  public String writeAllMyBucketsToXml() {
    //writes once
    writeXMLHeader();
    //writes once
    writeAllMyBucketsHead();
    //writes once
    writeOwner();
    //writes MULTIPLE
    writeBuckets();
    //writes once
    writeAllMyBucketsTail();
    return finalOutput.toString();
  }
  
  public String writeListBucketToXml(final String bucketName) {
    //writes once
    writeXMLHeader();
    //writes once
    writeListBucketHead();
    //writes once
    writeListBucketSubHead(bucketName);
    //writes MULTIPLE
    writeContents();
    //writes once
    writeListBucketsTail();
    return finalOutput.toString();
  }

  public String writeListPartsToXml(final String bucketName,
                                    final String objectName,
                                    final String userName,
                                    final String uploadId,
                                    final String partNumberMarker,
                                    final String maxParts) {
    //writes once
    writeXMLHeader();
    //writes once
    writeListPartsHead();
    //writes once
    writeListPartsSubHead(bucketName, objectName, userName, uploadId,
                          partNumberMarker, maxParts);
    //writes MULTIPLE
    writeParts();
    //writes once
    writeListPartsTail();
    return finalOutput.toString();
  }

  public String writeMultiPartCompleteToXml(final String bucketName,
                                            final String objectName,
                                            final String serviceHost,
                                            final String proxyPort) {
    //writes once
    writeXMLHeader();
    //writes once
    writeCompleteMultiPartHead();
    //writes once
    writeCompleteMultiPartBody(bucketName, objectName, serviceHost, proxyPort);
    //writes once
    writeCompleteMultiPartTail();
    return finalOutput.toString();
  }

  public String writeListVersionsToXml(final Map<String, List<S3HdfsFileStatus>> versions,
                                       final String bucketName) {
    //writes once
    writeXMLHeader();
    //write once
    writeListVersionsHead();
    //write once
    writeListVersionsSubHead(bucketName, versions);
    //write MULTIPLE
    writeVersionsOrDeleteMarkers(versions);
    //write once
    writeListVersionsTail();
    return finalOutput.toString();
  }

  private void writeVersionsOrDeleteMarkers(Map<String, List<S3HdfsFileStatus>> versions) {
    for(Map.Entry<String, List<S3HdfsFileStatus>> entry : versions.entrySet()) {
      List<S3HdfsFileStatus> listOfVersions = entry.getValue();
      for (S3HdfsFileStatus version : listOfVersions) {
        if (version.isDelMarker()) {
          finalOutput.append("<DeleteMarker>");
        } else {
          finalOutput.append("<Version>");
        }
        finalOutput.append("<Key>").append(entry.getKey()).append("</Key>");
        finalOutput.append("<VersionId>").append(version.getVersionId()).append("</VersionId>");
        finalOutput.append("<IsLatest>").append(version.isLatest()).append("</IsLatest>");
        finalOutput.append("<LastModified>").append(convertS3Time(version.getObjectModTime())).append("</LastModified>");
        finalOutput.append("<ETag>\"HARDCODED1234567890\"</ETag>");
        finalOutput.append("<Size>").append(version.getObjectSize()).append("</Size>");
        finalOutput.append("<StorageClass>STANDARD</StorageClass>");
        finalOutput.append("<Owner>");
        finalOutput.append("<ID>\"HARDCODED1234567890\"</ID>");
        finalOutput.append("<DisplayName>").append(version.getOwner()).append("</DisplayName>");
        finalOutput.append("</Owner>");
        if (version.isDelMarker()) {
          finalOutput.append("</DeleteMarker>");
        } else {
          finalOutput.append("</Version>");
        }
      }
    }
  }

  private void writeListVersionsSubHead(String bucketName,
                                        Map<String, List<S3HdfsFileStatus>> versions) {
    int maxKeys = versions.size();
    finalOutput.append("<Name>").append(bucketName).append("</Name>");
    finalOutput.append("<Prefix></Prefix>").append("<KeyMarker></KeyMarker>");
    finalOutput.append("<MaxKeys>").append(maxKeys).append("</MaxKeys>");
    finalOutput.append("<IsTruncated>false</IsTruncated>");
  }

  private void writeListVersionsHead() {
    finalOutput.append("<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
  }

  private void writeListVersionsTail() {
    finalOutput.append("</ListVersionsResult>");
  }

  private void writeParts() {
    JsonNode array =
        origJSON.get("FileStatuses").get("FileStatus");

    for(int i = 0; i < array.size(); i++) {
      long size;
      String key;
      long lastMod;
      JsonNode element = array.get(i);
      key = element.get("pathSuffix").getTextValue();
      if(key.endsWith(PART_FILE_NAME)) {
        int partNum = Integer.parseInt(key.replace(PART_FILE_NAME, ""));
        size = element.get("length").getLongValue();
        lastMod = element.get("modificationTime").getLongValue();
        finalOutput.append("<Part>");
        finalOutput.append("<PartNumber>").append(partNum).append("</PartNumber>");
        finalOutput.append("<LastModified>").append(convertS3Time(lastMod)).append("</LastModified>");
        finalOutput.append("<ETag>\"HARDCODED1234567890\"</ETag>");
        finalOutput.append("<Size>").append(size).append("</Size>");
        finalOutput.append("</Part>");
      }
    }
  }

  private void writeListPartsSubHead(String bucketName, String objectName,
                                     String userName, String uploadId,
                                     String partNumberMarker, String maxParts) {
    writeBucket(bucketName);
    writeObject(objectName);
    finalOutput.append("<UploadId>").append(uploadId).append("</UploadId>");
    finalOutput.append("<Initiator>");
    finalOutput.append("<ID>HARDCODED1234567890</ID>");
    finalOutput.append("<DisplayName>").append(userName).append("</DisplayName>");
    finalOutput.append("</Initiator>");
    finalOutput.append("<Owner>");
    finalOutput.append("<ID>HARDCODED1234567890</ID>");
    finalOutput.append("<DisplayName>").append(userName).append("</DisplayName>");
    finalOutput.append("</Owner>");
    finalOutput.append("<StorageClass>STANDARD</StorageClass>");
    if(partNumberMarker != null)
      finalOutput.append("<PartNumberMarker>").append(partNumberMarker).append("</PartNumberMarker>");
    if(partNumberMarker != null)
      finalOutput.append("<MaxParts>").append(maxParts).append("</MaxParts>");
    finalOutput.append("<IsTruncated>false</IsTruncated>");
  }

  private void writeListPartsTail() {
    finalOutput.append("</ListPartsResult>");
  }

  private void writeListPartsHead() {
    finalOutput.append("<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
  }

  private void writeContents() {
    String eTag = "HARDCODED1234567890";
    String idNumber = "HARDCODED1234567890";

    JsonNode array =
        origJSON.get("FileStatuses").get("FileStatus");

    for(int i = 0; i < array.size(); i++) {
      String name;
      long size;
      String key;
      long lastMod;
      JsonNode element = array.get(i);
        name = element.get("owner").getTextValue();
        key = element.get("pathSuffix").getTextValue();
        size = element.get("length").getLongValue();
        lastMod = element.get("modificationTime").getLongValue();
      finalOutput.append("<Contents>");
      finalOutput.append("<Key>").append(key).append("</Key>");
      finalOutput.append("<LastModified>").append(convertS3Time(lastMod)).append("</LastModified>");
      finalOutput.append("<ETag>").append(eTag).append("</ETag>");
      finalOutput.append("<Size>").append(size).append("</Size>");
      finalOutput.append("<Owner>");
      finalOutput.append("<ID>").append(idNumber).append("</ID>");
      finalOutput.append("<DisplayName>").append(name).append("</DisplayName>");
      finalOutput.append("</Owner>");
      finalOutput.append("<StorageClass>STANDARD</StorageClass>");
      finalOutput.append("</Contents>");
    }
  }

  private void writeXMLHeader() {
    finalOutput.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
  }

  private void writeListBucketHead() {
    finalOutput.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
  }

  private void writeListBucketsTail() {
    finalOutput.append("</ListBucketResult>");
  }

  private void writeAllMyBucketsHead() {
    finalOutput.append("<ListAllMyBucketsResult " +
        "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
  }

  private void writeAllMyBucketsTail() {
    finalOutput.append("</ListAllMyBucketsResult>");
  }

  private void writeListBucketSubHead(String bucketName) {
    int maxKeys = origJSON.get("FileStatuses").get("FileStatus").size();
    finalOutput.append("<Name>").append(bucketName).append("</Name>");
    finalOutput.append("<Prefix></Prefix>").append("<Marker></Marker>");
    finalOutput.append("<MaxKeys>").append(maxKeys).append("</MaxKeys>");
    finalOutput.append("<IsTruncated>false</IsTruncated>");
  }

  private void writeOwner() {
    String idNumber = "HARDCODED1234567890";
    String ownerName = userName;
    finalOutput.append("<Owner>");
    finalOutput.append("<ID>").append(idNumber).append("</ID>");
    finalOutput.append("<DisplayName>").append(ownerName).append("</DisplayName>");
    finalOutput.append("</Owner>");
  }

  private void writeBuckets() {
    // parse the HDFS fileStatus JSON array
    JsonNode array =
        origJSON.get("FileStatuses").get("FileStatus");

    // open buckets bracket
    finalOutput.append("<Buckets>");

    // write individual buckets from FileStatus
    for(int i = 0; i < array.size(); i++) {
      JsonNode element = array.get(i);
      String name = element.get("pathSuffix").getTextValue();
      long creationDate = element.get("modificationTime").getLongValue();
      finalOutput.append("<Bucket>");
      finalOutput.append("<Name>").append(name).append("</Name>");
      finalOutput.append("<CreationDate>").append(convertS3Time(creationDate)).append("</CreationDate>");
      finalOutput.append("</Bucket>");
    }
    // close buckets bracket
    finalOutput.append("</Buckets>");
  }

  private void writeUploadId() {
    finalOutput.append("<UploadId>").append(UUID.randomUUID()).append("</UploadId>");
  }

  private void writeObject(String objectKey) {
    finalOutput.append("<Key>").append(objectKey).append("</Key>");
  }

  private void writeBucket(String bucketName) {
    finalOutput.append("<Bucket>").append(bucketName).append("</Bucket>");
  }

  private void writeInitiateMultiPartTail() {
    finalOutput.append("</InitiateMultipartUploadResult>");
  }

  private void writeInitiateMultiPartHead() {
    finalOutput.append("<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
  }

  @SuppressWarnings("deprecation")
  private String convertS3Time(final long time) {
    // FORMAT: 2008-03-26T22:54:30.000Z
    Date date = new Date(time);
    String year = Integer.toString(date.getYear() + 1900);
    String month = Integer.toString(date.getMonth() + 1);
    if(month.length() < 2) {
      month = "0"+month;
    }
    String day = Integer.toString(date.getDate());
    if(day.length() < 2) {
      day = "0"+day;
    }
    String hours = Integer.toString(date.getHours()
        + (date.getTimezoneOffset() / 60));
    if(hours.length() < 2) {
      hours = "0"+hours;
    }
    String minutes = Integer.toString(date.getMinutes());
    if(minutes.length() < 2) {
      minutes = "0"+minutes;
    }
    String seconds = Integer.toString(date.getSeconds());
    if(seconds.length() < 2) {
      seconds = "0"+seconds;
    }
    String millisecs = Long.toString(date.getTime() % 1000);
    if(millisecs.length() < 2) {
      seconds = "00"+seconds;
    } else if (millisecs.length() < 3) {
      seconds = "0"+seconds;
    }
    return (year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":" +
      seconds + "." + millisecs + "Z");
  }

  private void writeCompleteMultiPartBody(String bucketName,
                                          String objectName,
                                          String serviceHost,
                                          String proxyPort) {
    finalOutput.append("<Location>http://").append(serviceHost).append(":")
               .append(proxyPort).append("/").append(bucketName).append("/")
               .append(objectName).append("</Location>");
    writeBucket(bucketName);
    writeObject(objectName);
    finalOutput.append("<ETag>HARDCODED1234567890</ETag>");
  }

  private void writeCompleteMultiPartTail() {
    finalOutput.append("</CompleteMultipartUploadResult>");
  }

  private void writeCompleteMultiPartHead() {
    finalOutput.append("<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
  }

}
