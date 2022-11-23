// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import org.mariadb.r2dbc.SslMode;

public class SslConfig {

  public static final SslConfig DISABLE_INSTANCE = new SslConfig(SslMode.DISABLE);

  private final SslMode sslMode;
  private String serverSslCert;
  private String clientSslCert;
  private String clientSslKey;
  private CharSequence clientSslPassword;
  private List<String> tlsProtocol;
  private SslContextBuilder sslContextBuilder;

  public SslConfig(
      SslMode sslMode,
      String serverSslCert,
      String clientSslCert,
      String clientSslKey,
      CharSequence clientSslPassword,
      List<String> tlsProtocol)
      throws R2dbcTransientResourceException {
    this.sslMode = sslMode;
    this.serverSslCert = serverSslCert;
    this.clientSslCert = clientSslCert;
    this.tlsProtocol = tlsProtocol;
    this.clientSslCert = clientSslCert;
    this.clientSslKey = clientSslKey;
    this.clientSslPassword = clientSslPassword;
    this.sslContextBuilder = getSslContextBuilder();
  }

  public SslConfig(SslMode sslMode) {
    this.sslMode = sslMode;
  }

  public SslMode getSslMode() {
    return sslMode;
  }

  private SslContextBuilder getSslContextBuilder() throws R2dbcTransientResourceException {
    final SslContextBuilder sslCtxBuilder = SslContextBuilder.forClient();

    if (sslMode == SslMode.TRUST) {
      sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
    } else {

      if (serverSslCert != null) {
        try {
          InputStream inStream = loadCert(serverSslCert);
          sslCtxBuilder.trustManager(inStream);
        } catch (FileNotFoundException fileNotFoundEx) {
          throw new R2dbcTransientResourceException(
              "Failed to find serverSslCert file. serverSslCert=" + serverSslCert,
              "08000",
              fileNotFoundEx);
        }
      } else {
        throw new R2dbcTransientResourceException(
            "Server certificate needed (option `serverSslCert`) for ssl mode " + sslMode, "08000");
      }
    }
    if (clientSslCert != null && clientSslKey != null) {
      InputStream certificatesStream;
      try {
        certificatesStream = loadCert(clientSslCert);
      } catch (FileNotFoundException fileNotFoundEx) {
        throw new R2dbcTransientResourceException(
            "Failed to find clientSslCert file. clientSslCert=" + clientSslCert,
            "08000",
            fileNotFoundEx);
      }

      try {
        InputStream privateKeyStream = new FileInputStream(clientSslKey);
        sslCtxBuilder.keyManager(
            certificatesStream,
            privateKeyStream,
            clientSslPassword == null ? null : clientSslPassword.toString());
      } catch (FileNotFoundException fileNotFoundEx) {
        throw new R2dbcTransientResourceException(
            "Failed to find clientSslKey file. clientSslKey=" + clientSslKey,
            "08000",
            fileNotFoundEx);
      }
    }

    if (tlsProtocol != null) {
      sslCtxBuilder.protocols(tlsProtocol.toArray(new String[tlsProtocol.size()]));
    }
    return sslCtxBuilder;
  }

  public SslContext getSslContext() throws R2dbcTransientResourceException, SSLException {
    return sslContextBuilder.build();
  }

  private InputStream loadCert(String path) throws FileNotFoundException {
    InputStream inStream = null;
    // generate a keyStore from the provided cert
    if (path.startsWith("-----BEGIN CERTIFICATE-----")) {
      inStream = new ByteArrayInputStream(path.getBytes());
    } else if (path.startsWith("classpath:")) {
      // Load it from a classpath relative file
      String classpathFile = path.substring("classpath:".length());
      inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathFile);
    } else {
      inStream = new FileInputStream(path);
    }
    return inStream;
  }

  @SuppressWarnings("static")
  public GenericFutureListener<Future<? super io.netty.channel.Channel>> getHostNameVerifier(
      CompletableFuture<Void> result,
      String host,
      long threadId,
      SSLEngine engine,
      Supplier<Boolean> closeChannelIfNeeded) {
    return future -> {
      if (!future.isSuccess()) {
        result.completeExceptionally(future.cause());
        return;
      }
      if (sslMode == SslMode.VERIFY_FULL) {
        try {
          DefaultHostnameVerifier hostnameVerifier = new DefaultHostnameVerifier();
          SSLSession session = engine.getSession();
          if (!hostnameVerifier.verify(host, session, threadId)) {

            // Use proprietary verify method in order to have an exception with a better description
            // of error.
            Certificate[] certs = session.getPeerCertificates();
            X509Certificate cert = (X509Certificate) certs[0];

            DefaultHostnameVerifier.verify(host, cert, threadId);
          }
        } catch (SSLException ex) {
          closeChannelIfNeeded.get();
          result.completeExceptionally(
              new R2dbcNonTransientResourceException(
                  "SSL hostname verification failed : " + ex.getMessage(), "08006"));
          return;
        }
      }
      result.complete(null);
    };
  }

  @Override
  public String toString() {
    return "SslConfig{"
        + "sslMode="
        + sslMode
        + ", serverSslCert="
        + serverSslCert
        + ", clientSslCert="
        + clientSslCert
        + ", tlsProtocol="
        + tlsProtocol
        + ", clientSslKey="
        + clientSslKey
        + '}';
  }
}
