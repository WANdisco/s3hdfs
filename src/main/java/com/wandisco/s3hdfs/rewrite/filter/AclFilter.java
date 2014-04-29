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
package com.wandisco.s3hdfs.rewrite.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import java.io.IOException;

public class AclFilter implements Filter {
  private static Logger LOG = LoggerFactory.getLogger(AclFilter.class);

  private FilterConfig filterConfig;

  public void init(FilterConfig filterConfig) throws ServletException {
    this.filterConfig = filterConfig;
  }

  public void doFilter(ServletRequest request,
                       ServletResponse response,
                       FilterChain chain) throws IOException, ServletException {
    LOG.debug("doFilter entered");
    chain.doFilter(request, response);
  }

  public void destroy() {
  }

  /**
   * Initializer for the Filter
   */
  static public class Initializer extends FilterInitializer {
    public Initializer() {
    }

    @Override // FilterInitializer
    public void initFilter(FilterContainer container, Configuration conf) {
      String filterName = AclFilter.class.getName();
      container.addGlobalFilter(filterName, filterName, null);
    }
  }
}
