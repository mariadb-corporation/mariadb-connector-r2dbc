// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.io.*;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

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
  private UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer;

  private boolean sslTunnelDisableHostVerification;

  public SslConfig(
      SslMode sslMode,
      String serverSslCert,
      String clientSslCert,
      String clientSslKey,
      CharSequence clientSslPassword,
      List<String> tlsProtocol,
      boolean sslTunnelDisableHostVerification,
      UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer)
      throws R2dbcTransientResourceException {
    this.sslMode = sslMode;
    this.serverSslCert = serverSslCert;
    this.clientSslCert = clientSslCert;
    this.tlsProtocol = tlsProtocol;
    this.clientSslKey = clientSslKey;
    this.clientSslPassword = clientSslPassword;
    this.sslTunnelDisableHostVerification = sslTunnelDisableHostVerification;
    this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
    if (sslMode != SslMode.DISABLE) {
      this.sslContextBuilder = getSslContextBuilder();
    }
  }

  public SslConfig(SslMode sslMode) {
    this.sslMode = sslMode;
  }

  public SslMode getSslMode() {
    return sslMode;
  }

  public boolean sslTunnelDisableHostVerification() {
    return this.sslTunnelDisableHostVerification;
  }

  private SslContextBuilder getSslContextBuilder() throws R2dbcTransientResourceException {
    final SslContextBuilder sslCtxBuilder = SslContextBuilder.forClient();

    if (sslMode == SslMode.TRUST || sslMode == SslMode.TUNNEL) {
      sslCtxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
    } else {

      if (serverSslCert != null) {
        InputStream inStream = null;
        try {
          inStream = loadCert(serverSslCert);
          sslCtxBuilder.trustManager(inStream);
        } catch (FileNotFoundException fileNotFoundEx) {
          throw new R2dbcTransientResourceException(
              "Failed to find serverSslCert file. serverSslCert=" + serverSslCert,
              "08000",
              fileNotFoundEx);
        } finally {
          if (inStream != null) {
            try {
              inStream.close();
            } catch (IOException e) {
            }
          }
        }
      } else {
        // use the system default, i.e. verify against PKI
        sslCtxBuilder.trustManager((TrustManagerFactory) null);
      }
    }
    if (clientSslCert != null && clientSslKey != null) {
      InputStream certificatesStream = null;
      try {
        certificatesStream = loadCert(clientSslCert);
      } catch (FileNotFoundException fileNotFoundEx) {
        if (certificatesStream != null) {
          try {
            certificatesStream.close();
          } catch (IOException e) {
          }
        }
        throw new R2dbcTransientResourceException(
            "Failed to find clientSslCert file. clientSslCert=" + clientSslCert,
            "08000",
            fileNotFoundEx);
      }

      InputStream privateKeyStream = null;
      try {
        privateKeyStream = loadCert(clientSslKey);
        sslCtxBuilder.keyManager(
            certificatesStream,
            privateKeyStream,
            clientSslPassword == null ? null : clientSslPassword.toString());
      } catch (FileNotFoundException fileNotFoundEx) {
        throw new R2dbcTransientResourceException(
            "Failed to find clientSslKey file. clientSslKey=" + clientSslKey,
            "08000",
            fileNotFoundEx);
      } finally {
        if (privateKeyStream != null) {
          try {
            privateKeyStream.close();
          } catch (IOException e) {
          }
        }
      }
    }

    if (tlsProtocol != null) {
      sslCtxBuilder.protocols(tlsProtocol.toArray(new String[tlsProtocol.size()]));
    }

    if (sslContextBuilderCustomizer == null) {
      return sslCtxBuilder;
    }
    return sslContextBuilderCustomizer.apply(sslCtxBuilder);
  }

  public SslContext getSslContext() throws R2dbcTransientResourceException, SSLException {
    return sslContextBuilder.build();
  }

  private InputStream loadCert(String path) throws FileNotFoundException {
    InputStream inStream;
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    if (sslMode != SslMode.DISABLE) {
      sb.append("sslMode=").append(sslMode.value);
      first = false;
    }
    if (serverSslCert != null) {
      if (!first) sb.append("&");
      sb.append("serverSslCert=").append(serverSslCert);
      first = false;
    }
    if (clientSslCert != null) {
      if (!first) sb.append("&");
      sb.append("clientSslCert=").append(clientSslCert);
      first = false;
    }
    if (tlsProtocol != null) {
      if (!first) sb.append("&");
      sb.append("tlsProtocol=").append(String.join(",", tlsProtocol));
      first = false;
    }
    if (clientSslKey != null) {
      if (!first) sb.append("&");
      sb.append("clientSslKey=").append(clientSslKey);
      first = false;
    }
    if (clientSslPassword != null) {
      if (!first) sb.append("&");
      sb.append("clientSslPassword=***");
      first = false;
    }

    if (sslTunnelDisableHostVerification) {
      if (!first) sb.append("&");
      sb.append("sslTunnelDisableHostVerification=true");
      first = false;
    }

    return sb.toString();
  }
}
