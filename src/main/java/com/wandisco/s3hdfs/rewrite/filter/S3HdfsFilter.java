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

import com.wandisco.s3hdfs.command.Command;
import com.wandisco.s3hdfs.conf.S3HdfsConfiguration;
import com.wandisco.s3hdfs.conf.S3HdfsConstants;
import com.wandisco.s3hdfs.proxy.S3HdfsProxy;
import com.wandisco.s3hdfs.rewrite.redirect.CopyFileRedirect;
import com.wandisco.s3hdfs.rewrite.redirect.MetadataFileRedirect;
import com.wandisco.s3hdfs.rewrite.redirect.VersionRedirect;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsRequestWrapper;
import com.wandisco.s3hdfs.rewrite.wrapper.S3HdfsResponseWrapper;
import com.wandisco.s3hdfs.rewrite.xml.S3XmlWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY;

/**
 * This is the main entry-point class that handles S3 HTTP requests and
 * processes them through various Command classes.
 */
public class S3HdfsFilter implements Filter {
  private static Logger LOG = LoggerFactory.getLogger(S3HdfsFilter.class);
  private static String fakeName; // used in testing

  // the namenode http port
  private String nameNodePort;
  // the s3hdfs service host name (localhost or myMachine)
  private String serviceHostName;
  // the s3hdfs root directory (/s3hdfs)
  private String s3HdfsRootDir;
  // the s3hdfs prefix (/webhdfs/v1/s3hdfs)
  private String s3HdfsPrefix;
  // the s3hdfs proxy port to use (80)
  private String s3HdfsProxyPort;
  // the s3hdfs proxy max connections (20)
  private String maxConnections;

  /**
   * A static method for adding the webHDFS URI prefix to anything.
   *
   * @param uri the URI to append webHDFS prefix to
   * @return the webHDFS prefixed URI
   */
  public static String ADD_WEBHDFS(final String uri) {
    if (uri.charAt(0) == '/')
      return S3HdfsConstants.WEBHDFS_PREFIX + uri;
    else
      return S3HdfsConstants.WEBHDFS_PREFIX + "/" + uri;
  }

  private static String endsIn(String bigString, String smallString) {
    int sl = smallString.length();
    int bl = bigString.length();
    if (bigString.regionMatches(true, bl - sl, smallString, 0, sl)) {
      return bigString.substring(0, bl - sl);
    } else {
      return null;
    }
  }

  // PJJ: this is a testing utility method
  static void setFakeUserName(String user) {
    fakeName = user;
  }

  @Override // Filter
  public void init(FilterConfig filterConfig) throws ServletException {
    nameNodePort =
        filterConfig.getInitParameter(DFS_NAMENODE_HTTP_PORT_KEY);
    s3HdfsProxyPort =
        filterConfig.getInitParameter(S3_PROXY_PORT_KEY);
    s3HdfsRootDir =
        filterConfig.getInitParameter(S3_DIRECTORY_KEY);
    serviceHostName =
        filterConfig.getInitParameter(S3_SERVICE_HOSTNAME_KEY);
    maxConnections =
        filterConfig.getInitParameter(S3_MAX_CONNECTIONS_KEY);

    dumpFilterConfig(filterConfig);

    if (serviceHostName == null || serviceHostName.length() == 0) {
      throw new ServletException(S3_SERVICE_HOSTNAME_KEY + " must be set.");
    }
    if (s3HdfsProxyPort == null || s3HdfsProxyPort.length() == 0) {
      throw new ServletException(S3_PROXY_PORT_KEY + " must be set.");
    }
    if (s3HdfsRootDir == null || s3HdfsRootDir.length() == 0) {
      throw new ServletException(S3_DIRECTORY_KEY + " must be set.");
    }
    if (nameNodePort == null || nameNodePort.length() == 0) {
      throw new ServletException(DFS_NAMENODE_HTTP_PORT_KEY + " must be set.");
    }
    if (maxConnections == null || maxConnections.length() == 0) {
      throw new ServletException(S3_MAX_CONNECTIONS_KEY + " must be set.");
    }

    // Attempt to start the Singleton S3HdfsProxy service.
    S3HdfsProxy.startInstance(
        Integer.decode(s3HdfsProxyPort),
        nameNodePort,
        serviceHostName,
        Integer.decode(maxConnections)
    );

    // Set S3HDFS URL prefix to allow through.
    s3HdfsPrefix = ADD_WEBHDFS(s3HdfsRootDir);
  }

  @Override // Filter
  public void doFilter(ServletRequest request,
                       ServletResponse response,
                       FilterChain chain) throws IOException, ServletException {
    // 1,1. Cast the request and response to HTTP.
    // NOTE: We wrap the response to grab status code.
    final S3HdfsRequestWrapper req =
        (request instanceof S3HdfsRequestWrapper) ?
            (S3HdfsRequestWrapper) request :
            new S3HdfsRequestWrapper((HttpServletRequest) request);
    final S3HdfsResponseWrapper res =
        (response instanceof S3HdfsResponseWrapper) ?
            (S3HdfsResponseWrapper) response :
            new S3HdfsResponseWrapper((HttpServletResponse) response);

    // 1,2. Only allow S3HDFS headered or S3HDFS prefixed requests through.
    String s3Header = req.getHeader(S3_HEADER_NAME);
    if ((s3Header == null || !s3Header.equalsIgnoreCase(S3_HEADER_VALUE)) &&
        !req.getRequestURI().startsWith(s3HdfsPrefix)) {
      // Not doing S3HDFS operation -- send to WebHDFS.
      chain.doFilter(req, res);
      return;
    } else if (req.getRequestURI() != null &&
        req.getRequestURI().equalsIgnoreCase(S3_CHECK_URI)) {
      // Doing a RESTFUL check -- return XML.
      PrintWriter writer = res.getWriter();
      writer.write(S3XmlWriter.writeCheckToXml());
      writer.close();
      return;
    }

    // Main processing phase!
    // 2. Check if already prefixed for webHDFS, pass through.
    if (req.getRequestURI().startsWith(s3HdfsPrefix)) {

      boolean versioning = false;

      // 2,1a. Return metadata if this file had any.
      // Metadata must be obtained prior to passing through to webHDFS.
      // Note: It is possible that req and res are wrapped -- getQS will differ.
      // Note: Using getParameter() ensures it is not wrapped.
      if (req.getParameter("op") != null &&
          req.getParameter("op").equals("OPEN") &&
          req.getPathInfo().endsWith(OBJECT_FILE_NAME) &&
          req.getMethod().equals("GET")) {
        MetadataFileRedirect metadataFileRedirect =
            new MetadataFileRedirect(req);
        Properties metadata = metadataFileRedirect.sendRead(
            getNameNodeAddress(), getUserName(req));
        if (metadata != null) {
          for (String key : metadata.stringPropertyNames()) {
            res.setHeader(key, metadata.getProperty(key));
          }
        }
      }

      if (req.getParameter("op") != null &&
          req.getParameter("op").equals("CREATE") &&
          req.getPathInfo().endsWith(OBJECT_FILE_NAME) &&
          req.getMethod().equals("PUT")) {
        VersionRedirect versionFileRedirect =
            new VersionRedirect(req, res, null);
        versioning = versionFileRedirect.check(getNameNodeAddress(), getUserName(req));
        if (versioning) {
          versionFileRedirect.updateVersion(getNameNodeAddress(), getUserName(req));
        }
      }

      if (req.getParameter("op") != null &&
          req.getParameter("op").equals("CREATE") &&
          req.getPathInfo().endsWith(OBJECT_FILE_NAME) &&
          req.getMethod().equals("PUT") &&
          req.getHeader("x-amz-copy-source") != null &&
          !req.getHeader("x-amz-copy-source").isEmpty()) {
        String header =
            URLDecoder.decode(req.getHeader("x-amz-copy-source"), DEFAULT_CHARSET);
        CopyFileRedirect copyFileRedirect =
            new CopyFileRedirect(req, res);
        copyFileRedirect.sendCopy(getNameNodeAddress(), getUserName(req),
            stripBucketName(header), stripObjectName(header));
      }

      // 2,1b. Pass through to webHDFS and obtain response.
      LOG.info("doFilter: URI already has webhdfs prefix. Passing through.");
      System.out.println(req.getRequestURL().toString());
      chain.doFilter(req, res);
      LOG.info("Returned from webHDFS: " + res.toString());

      if (req.getParameter("op") != null &&
          req.getParameter("op").equals("CREATE") &&
          req.getPathInfo().endsWith(OBJECT_FILE_NAME) &&
          req.getMethod().equals("PUT") &&
          req.getHeader("x-amz-copy-source") != null &&
          !req.getHeader("x-amz-copy-source").isEmpty()) {
        CopyFileRedirect copyFileRedirect =
            new CopyFileRedirect(req, res);
        copyFileRedirect.completeCopy();
      }

      // 2,1c. If we are uploading a part, set an ETag in the response.
      if (req.getQueryString().contains("op=CREATE") &&
          req.getPathInfo().endsWith(PART_FILE_NAME) &&
          req.getMethod().equals("PUT")) {
        res.setHeader("ETag", "HARDCODED1234567890");
        LOG.info("Set ETag in response.");
      }

      // 2,1d. Else if we are putting a new object in, then we need create
      // metadata AFTER the file has been created.
      else if (req.getParameter("op") != null &&
          req.getParameter("op").equals("CREATE") &&
          req.getPathInfo().endsWith(OBJECT_FILE_NAME) &&
          req.getMethod().equals("PUT")) {
        MetadataFileRedirect metadataFileRedirect =
            new MetadataFileRedirect(req);
        metadataFileRedirect.sendCreate(getNameNodeAddress(), getUserName(req));
        if (versioning) {
          VersionRedirect versionFileRedirect =
              new VersionRedirect(req, res, null);
          String version =
              versionFileRedirect.createVersion(getNameNodeAddress(), getUserName(req));
          res.setHeader("x-amz-version-id", version);
        }
        LOG.info("Created metadata.");
      }
    } else {
      // 2,2. Do the logic processing of S3 requests to webHDFS.
      // This involves splitting up what we are doing and figuring out URI,
      // bucketName, objectName, and how to sendInitiate a proper webHDFS request.
      System.out.println(req.getRequestURL().toString());
      processS3(req, res, chain);
      System.out.println("Returned from processing.");
    }

    // Post-processing!
    // 3a. Check if response is a 201; set to 200 and clear Location header.
    if (res.getStatus() == 201) {
      res.setStatus(200);
    }
    // 3b. Check if partial response; set to 206 with headers.
    if (res.getStatus() == 200 && req.getHeader("Range") != null) {
      // Parse ranges from request.
      long[] ranges = parseRange(req);
      long startRange = ranges[0];
      long endRange = startRange + ranges[1];
      // Convert response to partial content.
      res.setHeader("Accept-Ranges", "bytes");
      res.setHeader("Content-Range", "bytes " + startRange + "-" + endRange + "/443");
      res.setStatus(206);
    }
    // 3c. Add Date header and close connection.
    res.setDateHeader("Date", System.currentTimeMillis());
    System.out.println("FINAL RESPONSE: " + res.toString());
  }

  /**
   * The entry logic method. Parses basic knowledge and
   * begins parsing requests into GET, PUT, POST, and DELETE.
   *
   * @param request  The original request casted to HTTP.
   * @param response The original response casted to HTTP.
   * @param chain    The filter chain.
   * @throws IOException
   * @throws ServletException
   */
  private void processS3(final S3HdfsRequestWrapper request,
                         final S3HdfsResponseWrapper response,
                         final FilterChain chain)
      throws IOException, ServletException {
    // 1. Parse initial information
    // TODO: This parsing needs to be alot nicer. Redo "getBucket&ModURI".
    final String[] rv = getBucketAndModifiedURI(request);
    final String bucketName = rv[0];
    final String modifiedURI = rv[1];
    final String objectName = rv[3];
    final String partNumber = getPartNumber(request);
    final String userName = getUserName(request);
    final String rootDir = getS3HdfsRootDirectory();
    final String serviceHost = getServiceHostName();
    final String proxyPort = getS3HdfsProxyPort();
    final String version = getVersion(request);

    LOG.info("doFilter: bucketName=" + bucketName + ", objectName=" +
        objectName + ", userName=" + userName);

    if (bucketName == null || modifiedURI == null)
      throw new IOException("Bucket name or modified URI is null.");

    // 2. Figure out what to do with the entry request.
    // We do this by making use of a Command class which breaks the logic
    // internally and handles the call in doCommand().
    Command command = Command.make(request, response, serviceHost, proxyPort,
        bucketName, objectName, userName,
        rootDir, version, partNumber);

    // 3. Make the call to do the command!
    // This results into a parseCommand(), parseURI, and doCommand() call
    // for this Command object.
    command.doCommand();
  }

  /**
   * Returns an array of two Strings - bucketName and modifiedURI
   * <p/>
   * Extracting the bucket name. In the S3 protocol, bucket name
   * can be extracted in the following manner (Note that in all
   * of these examples, the serviceHostName is s3.amazonaws.com):
   * <p/>
   * 1. The Host: header
   * If the Host: header is present, then we check if the
   * Host: header value has serviceHostName at the tail end.
   * e.g. Host: images.wandisco.net.s3.amazonaws.com
   * In this case the bucket name is images.wandisco.net
   * <p/>
   * If the Host: header does not end in serviceHostName,
   * then the complete header value is the bucket name,
   * e.g. Host: images.wandisco.net
   * In this example, the bucketname is images.wandisco.net
   * <p/>
   * 2. The part of the server name before the serviceHostName,
   * for example, if the service is available at
   * https://s3.amazonaws.com/, the a URL such as
   * https://mybucket.s3.amazonaws.com/myobject would mean
   * that the bucket name is mybucket, and the object name
   * is myobject. Wildcard DNS entries can be made such that
   * all *.s3.amazonaws.com point to this host. This is also
   * the reason that we need a configuration parameter
   * serviceHostName - we cannot depend on DNS reverse lookup
   * to determine the serviceHostName.
   * <p/>
   * 3. If the Host: header is absent, and the URL's hostname
   * equals the serviceHostName, for example a URL such as:
   * https://s3.amazonaws.com/images.wandisco.net/image1.png
   * the bucket name is images.wandisco.net as derived from
   * the first part of the URI
   */
  private String[] getBucketAndModifiedURI(final HttpServletRequest req) {
    String bucketName = null;
    String objectName = null;
    String modifiedURI = null;

    String hostHeader = req.getHeader("Host");
    hostHeader = hostHeader.trim();

    String host = req.getServerName();
    host = host.trim();

    String reqURI = req.getRequestURI();

    if (hostHeader == null || hostHeader.length() == 0) {
      if (host.contains(serviceHostName)) {
        /*
         * hostname from URL is the same as serviceHostName
         * then, bucketname is the first part of the URI
         */
        LOG.debug("bucketName came from 1");
        return bucketNameFromURI(reqURI);
      } else {
        /* check if hostname from URL has the serviceHostName at its tail end */
        bucketName = endsIn(host, "." + serviceHostName);
        if (bucketName == null) {
          LOG.debug("bucketName came from 2");
          /* no? then bucketname is the first part of the URI */
          return bucketNameFromURI(reqURI);
        } else {
          LOG.debug("bucketName came from 3");
          /* yes? then URI does not need to be modified */
          modifiedURI = ADD_WEBHDFS(reqURI);
        }
      }
    } else {
      /* check if Host: header has the serviceHostName at its tail end */
      LOG.debug("bucketName came from 4?");
      bucketName = endsIn(host, "." + serviceHostName);

      if (bucketName == null) {
        LOG.debug("bucketName came from 5");
        bucketName = stripBucketName(reqURI);
        objectName = stripObjectName(reqURI);
      } else {
        // we use this method to get the first URI entry... should be changed.
        objectName = stripBucketName(reqURI);
      }
      // ^ PJJ: this was not returning bucketName originally
      modifiedURI = ADD_WEBHDFS(reqURI);
    }

    String[] rv = new String[4];
    rv[0] = bucketName;
    rv[1] = modifiedURI;
    rv[2] = hostHeader;
    rv[3] = objectName;
    return rv;
  }

  private String[] bucketNameFromURI(String uri) {
    String[] rv = new String[2];
    if (uri.charAt(0) == '/')
      uri = uri.substring(1);
    int ind = uri.indexOf('/');
    if (ind >= 0) {
      rv[0] = uri.substring(0, ind);
      rv[1] = ADD_WEBHDFS(uri.substring(ind + 1));
      return rv;
    } else {
      rv[0] = uri;
      rv[1] = ADD_WEBHDFS("");
      return rv;
    }
  }

  private String stripBucketName(final String uri) {
    // Regular S3 path -> /bucket/object, /bucket
    String retVal = uri;
    if (retVal.charAt(0) == '/')
      retVal = retVal.substring(1);
    int ind = retVal.indexOf('/');
    if (ind >= 0)
      retVal = retVal.substring(0, ind);
    return retVal;
  }

  /**
   * Possibilities are:
   * /bucketN/objectN
   * /objectN
   *
   * @param uri
   * @return
   */
  private String stripObjectName(final String uri) {
    LOG.debug("Stripping objectName from URI: " + uri);
    // Regular S3 path -> /bucket/object, /object
    String retVal = uri;
    if (retVal.charAt(0) == '/')
      retVal = retVal.substring(1);
    int ind = retVal.indexOf('/');
    if (ind >= 0)
      retVal = retVal.substring(ind + 1, retVal.length());
    else
      retVal = "";
    return retVal;
  }

  @Override // Filter
  public void destroy() {
    // Do nothing.
  }

  private void dumpFilterConfig(FilterConfig conf) {
    Enumeration enm = conf.getInitParameterNames();
    while (enm.hasMoreElements()) {
      String key = (String) enm.nextElement();
      String val = conf.getInitParameter(key);
      LOG.debug("FilterParam: " + key + "=" + val);
    }
  }

  private String getPartNumber(HttpServletRequest request) {
    return request.getParameter("partNumber");
  }

  private String getVersion(HttpServletRequest request) {
    String version = request.getParameter("versionId");
    return version == null ? DEFAULT_VERSION : version;
  }

  public String getNameNodeAddress() {
    return getServiceHostName() + ":" + getNameNodePort();
  }

  public String getNameNodePort() {
    return nameNodePort;
  }

  public String getS3HdfsRootDirectory() {
    return s3HdfsRootDir;
  }

  public String getS3HdfsProxyPort() {
    return s3HdfsProxyPort;
  }

  public String getServiceHostName() {
    return serviceHostName;
  }

  public String getMaxConnections() {
    return maxConnections;
  }

  // SHV!!!
  // "simple" authentication requires a request parameter user.name=<user>
  // This is temporary implementation until
  // AWSAuthenticationHandler is made to work.
  // Then adding user.name parameters will not be necessary, since
  // authentication will go through AWSAuthenticationHandler, rather than
  // being "simple".
  public String getUserName(HttpServletRequest request) {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      LOG.warn("Current user is not available.", e);
      return null;
    }
    if (fakeName == null) {
      String name = request.getParameter("user.name");
      if (name == null) {
        name = ugi.getUserName();
      }
      return name;
    } else
      return fakeName;
  }

  private long[] parseRange(final HttpServletRequest request) {
    String rangeHeader = request.getHeader("Range");
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

  /**
   * Initializer for the Filter
   */
  static public class Initializer extends FilterInitializer {
    public Initializer() {
    }

    @Override // FilterInitializer
    public void initFilter(FilterContainer container, Configuration conf) {
      String filterName = S3HdfsFilter.class.getSimpleName();
      String filterClass = S3HdfsFilter.class.getName();

      String nameNodePort = conf.get(DFS_NAMENODE_HTTP_PORT_KEY,
          String.valueOf(DFS_NAMENODE_HTTP_PORT_DEFAULT));
      String serviceHostName = conf.get(S3_SERVICE_HOSTNAME_KEY);
      String s3Directory = conf.get(S3_DIRECTORY_KEY, S3_DIRECTORY_DEFAULT);
      String s3ProxyPort = conf.get(S3_PROXY_PORT_KEY, S3_PROXY_PORT_DEFAULT);
      String maxConnections = conf.get(S3_MAX_CONNECTIONS_KEY,
          S3_MAX_CONNECTIONS_DEFAULT);

      S3HdfsConfiguration s3Conf = new S3HdfsConfiguration();
      HashMap<String, String> parameters = new HashMap<String, String>();

      /**
       * We try to load configuration properties from the Conf passed to us
       * first. If they are not set there (hdfs-site + core-site), then we check
       * s3hdfs-site + s3hdfs-default configuration files.
       */

      if (nameNodePort != null && nameNodePort.length() > 0) {
        parameters.put(DFS_NAMENODE_HTTP_PORT_KEY, nameNodePort);
      }

      if (serviceHostName != null && serviceHostName.length() > 0) {
        parameters.put(S3_SERVICE_HOSTNAME_KEY, serviceHostName);
      } else {
        serviceHostName = s3Conf.get(S3_SERVICE_HOSTNAME_KEY);
        if (serviceHostName != null && serviceHostName.length() > 0) {
          parameters.put(S3_SERVICE_HOSTNAME_KEY, serviceHostName);
        }
      }

      if (s3Directory != null && s3Directory.length() > 0) {
        parameters.put(S3_DIRECTORY_KEY, s3Directory);
      } else {
        s3Directory = s3Conf.get(S3_DIRECTORY_KEY);
        if (s3Directory != null && s3Directory.length() > 0) {
          parameters.put(S3_DIRECTORY_KEY, s3Directory);
        }
      }

      if (s3ProxyPort != null && s3ProxyPort.length() > 0) {
        parameters.put(S3_PROXY_PORT_KEY, s3ProxyPort);
      } else {
        s3ProxyPort = s3Conf.get(S3_PROXY_PORT_KEY, S3_PROXY_PORT_DEFAULT);
        if (s3ProxyPort != null && s3ProxyPort.length() > 0) {
          parameters.put(S3_PROXY_PORT_KEY, s3ProxyPort);
        }
      }

      if (maxConnections != null && maxConnections.length() > 0) {
        parameters.put(S3_MAX_CONNECTIONS_KEY, maxConnections);
      } else {
        maxConnections = s3Conf.get(S3_MAX_CONNECTIONS_KEY,
            S3_MAX_CONNECTIONS_DEFAULT);
        if (maxConnections != null && maxConnections.length() > 0) {
          parameters.put(S3_MAX_CONNECTIONS_KEY, maxConnections);
        }
      }

      container.addGlobalFilter(filterName, filterClass, parameters);
    }
  }

}
