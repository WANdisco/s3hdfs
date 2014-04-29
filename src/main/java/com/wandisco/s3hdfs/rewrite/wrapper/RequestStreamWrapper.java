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
import java.io.InputStream;
import javax.servlet.ServletInputStream;

/**
 * A ServletOutputStream wrapper that sets its output stream to the
 * constructor given output stream.
 */
public class RequestStreamWrapper extends ServletInputStream {

  private final InputStream _in;

  public RequestStreamWrapper(InputStream realStream) {
    this._in = realStream;
  }

  @Override
  public int read() throws IOException {
    return _in.read();
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return _in.read(bytes);
  }

  @Override
  public int read(byte[] bytes, int i, int i2) throws IOException {
    return _in.read(bytes, i, i2);
  }

  @Override
  public long skip(long l) throws IOException {
    return _in.skip(l);
  }

  @Override
  public int available() throws IOException {
    return _in.available();
  }

  @Override
  public void close() throws IOException {
    _in.close();
  }

  @Override
  public synchronized void mark(int i) {
    _in.mark(i);
  }

  @Override
  public synchronized void reset() throws IOException {
    _in.reset();
  }

  @Override
  public boolean markSupported() {
    return _in.markSupported();
  }

  @Override
  public String toString() {
    return _in.toString();
  }
}
