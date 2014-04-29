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
package com.wandisco.s3hdfs.auth;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.SignatureException;
import java.util.Properties;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.wandisco.s3hdfs.conf.S3HdfsConstants.DEFAULT_CHARSET;

public class AWSAuthenticationHandler implements AuthenticationHandler {
  public final static String AUTH_TYPE_KEY = AuthenticationFilter.AUTH_TYPE;

  private static Logger LOG = LoggerFactory.getLogger(AWSAuthenticationHandler.class);

  /**
   * Constant that identifies the authentication mechanism.
   */
  public static final String TYPE = "AWS";
  /**
   * HTTP header used by the AWS client endpoint during an authentication sequence.
   */
  public static final String AUTHORIZATION = "Authorization";

  /**
   * Returns the authentication type of the authentication handler, 'AWS'.
   * <p/>
   *
   * @return the authentication type of the authentication handler, 'AWS'.
   */
  @Override
  public String getType() {
    return TYPE;
  }

  /**
   * Verifies the AWS authentication header
   * <p/>
   *
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   *
   * @return an authentication token if the AWS authentication header is correct
   *         <code>null</code> if it is in progress (in this case the handler handles the response to the client).
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if the AWS authentication header is incorrect
   */
  @Override
  public AuthenticationToken authenticate(HttpServletRequest request, final HttpServletResponse response)
    throws IOException, AuthenticationException {
      String authorization = request.getHeader(AUTHORIZATION);
      LOG.debug("authenticate - authorization = " + authorization);
      if (authorization == null || !authorization.startsWith("AWS")) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        return null;
      } else {
        if (LOG.isDebugEnabled()) LOG.debug("authenticate - returning jagane");
        String[] splitAuth = authorization.split("\\s");
        if (splitAuth.length < 2) {
          LOG.warn("authenticate - auth string does not have enough info. "
                      + authorization);
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return null;
        }
        String[] splitKey = splitAuth[1].split(":");
        if (splitKey.length < 2) {
          LOG.warn("authenticate - auth string does not have key/signature. "
                      + authorization);
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return null;
        }
        String nameAndSecretAccessKey[] = getNameAndSecretAccessKey(splitKey[0]);
        if (nameAndSecretAccessKey == null 
            || nameAndSecretAccessKey[1].length() == 0) {
          LOG.warn("authenticate - cannot find secretAccessKey for accessKeyId "
                      + splitKey[0]);
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return null;
        }
        String hostHeader = request.getHeader("Host");
        try {
          verifySignature(request.getMethod(),
                        nameAndSecretAccessKey[1], splitKey[1], "HmacSHA1", 
                        hostHeader, request.getRequestURI(),
                        getCanonicalizedQueryString(request));
        } catch (Exception ex) {
          LOG.warn("verifySignature threw " + ex);
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return null;
        }
        return new AuthenticationToken(nameAndSecretAccessKey[0],
                                       nameAndSecretAccessKey[0],
                                       "AWS");
      }
  }

  private String getCanonicalizedQueryString(HttpServletRequest request) {
    return null;
  }

  /**
   * The request is authenticated if we can regenerate the same signature given
   * on the request.  Before calling this function make sure to set the header values
   * defined by the public values above.
   *
   * @param httpVerb  - the type of HTTP request (e.g., GET, PUT)
   * @param secretKey - value obtained from the AWSAccessKeyId
   * @param signature - the signature we are trying to recreate, note can be URL-encoded
   * @param method    - { "HmacSHA1", "HmacSHA256" }
   *
   * @throws SignatureException
   *
   * @return true if request has been authenticated, false otherwise
   * @throws UnsupportedEncodingException
   */
  public boolean verifySignature(String httpVerb,
      String secretKey,
      String signature,
      String method,
      String hostHeader,
      String httpRequestURI,
      String canonicalizedQueryString)
      throws SignatureException, UnsupportedEncodingException {

    if (null == httpVerb || null == secretKey || null == signature) return false;

    httpVerb  = httpVerb.trim();
    secretKey = secretKey.trim();
    signature = signature.trim();

    // -> first calculate the StringToSign after the caller has initialized all the header values
    String StringToSign = genStringToSign(httpVerb, hostHeader, httpRequestURI, canonicalizedQueryString);
    String calSig = calculateRFC2104HMAC(StringToSign, secretKey, method.equalsIgnoreCase( "HmacSHA1" ));

    // -> the passed in signature is defined to be URL encoded? (and it must be base64 encoded)
    int offset = signature.indexOf( "%" );
    if (-1 != offset) signature = URLDecoder.decode( signature, "UTF-8" );

    boolean match = signature.equals( calSig );
    return match;
  }

  /**
   * This function generates the single string that will be used to sign with a users
   * secret key.
   *
   * StringToSign = HTTP-Verb + "\n" +
   * ValueOfHostHeaderInLowercase + "\n" +
   * HTTPRequestURI + "\n" +
   * CanonicalizedQueryString
   *
   * @return The single StringToSign or null.
   */
  private String genStringToSign(String httpVerb,
				 String hostHeader,
				 String httpRequestURI,
				 String canonicalizedQueryString) {
    StringBuffer stringToSign = new StringBuffer();

    stringToSign.append( httpVerb ).append( "\n" );

    if (null != hostHeader) stringToSign.append(hostHeader);
    stringToSign.append( "\n" );

    if (null != httpRequestURI) stringToSign.append(httpRequestURI);
    stringToSign.append( "\n" );

    if (null != canonicalizedQueryString) stringToSign.append(canonicalizedQueryString);

    if (0 == stringToSign.length())
      return null;
    else
      return stringToSign.toString();
  }

  /**
   * Create a signature by the following method:
   *     new String( Base64( SHA1 or SHA256 ( key, byte array )))
   *
   * @param signIt    - the data to generate a keyed HMAC over
   * @param secretKey - the user's unique key for the HMAC operation
   * @param useSHA1   - if false use SHA256
   * @return String   - the recalculated string
   * @throws SignatureException
   */
  private String calculateRFC2104HMAC( String signIt, String secretKey, boolean useSHA1 )
      throws SignatureException {
    SecretKeySpec key = null;
    Mac    hmacShaAlg = null;
    String result     = null;

    try {
      if ( useSHA1 ) {
        key = new SecretKeySpec( secretKey.getBytes(DEFAULT_CHARSET), "HmacSHA1" );
        hmacShaAlg = Mac.getInstance( "HmacSHA1" );
      } else {
        key = new SecretKeySpec( secretKey.getBytes(DEFAULT_CHARSET), "HmacSHA256" );
        hmacShaAlg = Mac.getInstance( "HmacSHA256" );
      }

      hmacShaAlg.init( key );
      byte [] rawHmac = hmacShaAlg.doFinal( signIt.getBytes(DEFAULT_CHARSET));
      result = new String( Base64.encodeBase64( rawHmac ), DEFAULT_CHARSET);

    } catch( Exception e ) {
      throw new SignatureException( "Failed to generate keyed HMAC on REST request: " + e.getMessage());
    }
    return result.trim();
  }

  /**
   * Releases any resources initialized by the authentication handler.
   */
  @Override
  public void destroy() {
  }

  @Override
  public boolean managementOperation(AuthenticationToken authenticationToken,
                                     HttpServletRequest httpServletRequest,
                                     HttpServletResponse httpServletResponse)
      throws IOException, AuthenticationException {
    return false;
  }

  /**
   * Initializes the authentication handler instance.
   * <p/>
   * This method is invoked by the {@link AuthenticationFilter#init} method.
   *
   * @param config configuration properties to initialize the handler.
   *
   * @throws ServletException thrown if the handler could not be initialized.
   */
  @Override
  public void init(Properties config) throws ServletException {
  }

  private static String[] tempHackCredentialsArray = {
   "AKIAD9F6L38VNF8MS6F9", "jagane", "kd8urKd984Kc8/Jf84Jgfu7F54dgbLd0Jf8gGf5l",
   "AKIAJKG98EJKFGER89AQ", "shv", "fg8945jkgkw8s/JKLD904JKDjs8ksjg873kxncur"};

  private static String[] getNameAndSecretAccessKey(String accessKeyId) {
    for (int i = 0; i < tempHackCredentialsArray.length; i += 3) {
      if (accessKeyId.equals(tempHackCredentialsArray[i])) {
        String[] rv = new String[2];
        rv[0] = tempHackCredentialsArray[i+1];
        rv[1] = tempHackCredentialsArray[i+2];
        return rv;
      }
    }
    return null;
  }
}
