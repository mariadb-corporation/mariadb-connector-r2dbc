// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.io.*;
import java.util.List;
import javax.net.ssl.SSLException;
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
    this.tlsProtocol = tlsProtocol;
    this.clientSslCert = clientSslCert;
    this.clientSslKey = clientSslKey;
    this.clientSslPassword = clientSslPassword;
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

  private SslContextBuilder getSslContextBuilder() throws R2dbcTransientResourceException {
    final SslContextBuilder sslCtxBuilder = SslContextBuilder.forClient();

    if (sslMode == SslMode.TRUST) {
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
        throw new R2dbcTransientResourceException(
            "Server certificate needed (option `serverSslCert`) for ssl mode " + sslMode, "08000");
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
    return sslCtxBuilder;
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
