/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.SslConfig;
import reactor.util.annotation.Nullable;

public final class MariadbConnectionConfiguration {

  public static final int DEFAULT_PORT = 3306;
  private final String database;
  private final String host;
  private final Duration connectTimeout;
  private final CharSequence password;
  private final int port;
  private final int prepareCacheSize;
  private final String socket;
  private final String username;
  private final boolean allowMultiQueries;
  private final boolean allowPipelining;
  private final Map<String, String> connectionAttributes;
  private final Map<String, String> sessionVariables;
  private final SslConfig sslConfig;
  private final String rsaPublicKey;
  private final String cachingRsaPublicKey;
  private final boolean allowPublicKeyRetrieval;
  private IsolationLevel isolationLevel;
  private final boolean useServerPrepStmts;

  private MariadbConnectionConfiguration(
      @Nullable Duration connectTimeout,
      @Nullable String database,
      @Nullable String host,
      @Nullable Map<String, String> connectionAttributes,
      @Nullable Map<String, String> sessionVariables,
      @Nullable CharSequence password,
      int port,
      @Nullable String socket,
      @Nullable String username,
      boolean allowMultiQueries,
      boolean allowPipelining,
      @Nullable List<String> tlsProtocol,
      @Nullable String serverSslCert,
      @Nullable String clientSslCert,
      @Nullable String clientSslKey,
      @Nullable CharSequence clientSslPassword,
      SslMode sslMode,
      @Nullable String rsaPublicKey,
      @Nullable String cachingRsaPublicKey,
      boolean allowPublicKeyRetrieval,
      boolean useServerPrepStmts,
      @Nullable Integer prepareCacheSize) {
    this.connectTimeout = connectTimeout == null ? Duration.ofSeconds(10) : connectTimeout;
    this.database = database;
    this.host = host;
    this.connectionAttributes = connectionAttributes;
    this.sessionVariables = sessionVariables;
    this.password = password;
    this.port = port;
    this.socket = socket;
    this.username = username;
    this.allowMultiQueries = allowMultiQueries;
    this.allowPipelining = allowPipelining;
    if (sslMode == SslMode.DISABLED) {
      this.sslConfig = SslConfig.DISABLE_INSTANCE;
    } else {
      this.sslConfig =
          new SslConfig(
              sslMode, serverSslCert, clientSslCert, clientSslKey, clientSslPassword, tlsProtocol);
    }
    this.rsaPublicKey = rsaPublicKey;
    this.cachingRsaPublicKey = cachingRsaPublicKey;
    this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
    this.useServerPrepStmts = useServerPrepStmts;
    this.prepareCacheSize = (prepareCacheSize == null) ? 250 : prepareCacheSize.intValue();
  }

  public static Builder fromOptions(ConnectionFactoryOptions connectionFactoryOptions) {
    Builder builder = new Builder();
    builder.connectTimeout(connectionFactoryOptions.getValue(CONNECT_TIMEOUT));
    builder.database(connectionFactoryOptions.getValue(DATABASE));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SOCKET)) {
      builder.socket(
          connectionFactoryOptions.getRequiredValue(MariadbConnectionFactoryProvider.SOCKET));
    } else {
      builder.host(connectionFactoryOptions.getRequiredValue(HOST));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES)) {
      builder.allowMultiQueries(
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.ALLOW_PIPELINING)) {
      builder.allowPipelining(
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.ALLOW_PIPELINING));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.USE_SERVER_PREPARE)) {
      builder.useServerPrepStmts(
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.USE_SERVER_PREPARE));
    }

    builder.prepareCacheSize(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.PREPARE_CACHE_SIZE));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SSL_MODE)) {
      builder.sslMode(
          Enum.valueOf(
              SslMode.class,
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SSL_MODE)));
    }
    builder.serverSslCert(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SERVER_SSL_CERT));
    builder.clientSslCert(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_CERT));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TLS_PROTOCOL)) {
      String[] protocols =
          connectionFactoryOptions
              .getValue(MariadbConnectionFactoryProvider.TLS_PROTOCOL)
              .split("[,;\\s]+");
      builder.tlsProtocol(protocols);
    }
    builder.password(connectionFactoryOptions.getValue(PASSWORD));
    builder.username(connectionFactoryOptions.getRequiredValue(USER));

    Integer port = connectionFactoryOptions.getValue(PORT);
    if (port != null) {
      builder.port(port);
    }

    Map<String, String> options =
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.OPTIONS);
    if (options != null) {
      builder.options(options);
    }
    return builder;
  }

  public static Builder builder() {
    return new Builder();
  }

  public IsolationLevel getIsolationLevel() {
    return isolationLevel;
  }

  protected void setIsolationLevel(IsolationLevel isolationLevel) {
    this.isolationLevel = isolationLevel;
  }

  @Nullable
  public Duration getConnectTimeout() {
    return this.connectTimeout;
  }

  @Nullable
  public String getDatabase() {
    return this.database;
  }

  @Nullable
  public String getHost() {
    return this.host;
  }

  @Nullable
  public Map<String, String> getConnectionAttributes() {
    return this.connectionAttributes;
  }

  @Nullable
  public Map<String, String> getSessionVariables() {
    return this.sessionVariables;
  }

  @Nullable
  public CharSequence getPassword() {
    return this.password;
  }

  public int getPort() {
    return this.port;
  }

  @Nullable
  public String getSocket() {
    return this.socket;
  }

  public String getUsername() {
    return this.username;
  }

  public boolean allowMultiQueries() {
    return allowMultiQueries;
  }

  public boolean allowPipelining() {
    return allowPipelining;
  }

  public SslConfig getSslConfig() {
    return sslConfig;
  }

  public String getRsaPublicKey() {
    return rsaPublicKey;
  }

  public String getCachingRsaPublicKey() {
    return cachingRsaPublicKey;
  }

  public boolean allowPublicKeyRetrieval() {
    return allowPublicKeyRetrieval;
  }

  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  public int getPrepareCacheSize() {
    return prepareCacheSize;
  }

  @Override
  public String toString() {
    StringBuilder hiddenPwd = new StringBuilder();
    if (password != null) {
      for (int i = 0; i < password.length(); i++) {
        hiddenPwd.append("*");
      }
    }
    return "MariadbConnectionConfiguration{"
        + "database='"
        + database
        + '\''
        + ", host='"
        + host
        + '\''
        + ", connectTimeout="
        + connectTimeout
        + ", password="
        + hiddenPwd.toString()
        + ", port="
        + port
        + ", socket='"
        + socket
        + '\''
        + ", username='"
        + username
        + '\''
        + ", allowMultiQueries="
        + allowMultiQueries
        + ", connectionAttributes="
        + connectionAttributes
        + ", sslConfig="
        + sslConfig
        + ", serverRsaPublicKeyFile='"
        + rsaPublicKey
        + '\''
        + ", allowPublicKeyRetrieval="
        + allowPublicKeyRetrieval
        + ", useServerPrepStmts="
        + useServerPrepStmts
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MariadbConnectionConfiguration that = (MariadbConnectionConfiguration) o;
    return port == that.port
        && allowMultiQueries == that.allowMultiQueries
        && allowPublicKeyRetrieval == that.allowPublicKeyRetrieval
        && Objects.equals(database, that.database)
        && Objects.equals(host, that.host)
        && Objects.equals(connectTimeout, that.connectTimeout)
        && Objects.equals(password, that.password)
        && Objects.equals(socket, that.socket)
        && Objects.equals(username, that.username)
        && Objects.equals(connectionAttributes, that.connectionAttributes)
        && Objects.equals(sslConfig, that.sslConfig)
        && Objects.equals(rsaPublicKey, that.rsaPublicKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        database,
        host,
        connectTimeout,
        password,
        port,
        socket,
        username,
        allowMultiQueries,
        connectionAttributes,
        sslConfig,
        rsaPublicKey,
        allowPublicKeyRetrieval);
  }

  /**
   * A builder for {@link MariadbConnectionConfiguration} instances.
   *
   * <p><i>This class is not threadsafe</i>
   */
  public static final class Builder implements Cloneable {

    @Nullable private String rsaPublicKey;
    @Nullable private String cachingRsaPublicKey;
    private boolean allowPublicKeyRetrieval;
    @Nullable private String username;
    @Nullable private Duration connectTimeout;
    @Nullable private String database;
    @Nullable private String host;
    @Nullable private Map<String, String> sessionVariables;
    @Nullable private Map<String, String> connectionAttributes;
    @Nullable private CharSequence password;
    private int port = DEFAULT_PORT;
    @Nullable private String socket;
    private boolean allowMultiQueries = false;
    private boolean allowPipelining = true;
    private boolean useServerPrepStmts = false;
    @Nullable Integer prepareCacheSize;
    @Nullable private List<String> tlsProtocol;
    @Nullable private String serverSslCert;
    @Nullable private String clientSslCert;
    @Nullable private String clientSslKey;
    @Nullable private CharSequence clientSslPassword;
    private SslMode sslMode = SslMode.DISABLED;

    private Builder() {}

    /**
     * Returns a configured {@link MariadbConnectionConfiguration}.
     *
     * @return a configured {@link MariadbConnectionConfiguration}
     */
    public MariadbConnectionConfiguration build() {

      if (this.host == null && this.socket == null) {
        throw new IllegalArgumentException("host or socket must not be null");
      }

      if (this.host != null && this.socket != null) {
        throw new IllegalArgumentException(
            "Connection must be configured for either host/port or socket usage but not both");
      }

      if (this.username == null) {
        throw new IllegalArgumentException("username must not be null");
      }

      return new MariadbConnectionConfiguration(
          this.connectTimeout,
          this.database,
          this.host,
          this.connectionAttributes,
          this.sessionVariables,
          this.password,
          this.port,
          this.socket,
          this.username,
          this.allowMultiQueries,
          this.allowPipelining,
          this.tlsProtocol,
          this.serverSslCert,
          this.clientSslCert,
          this.clientSslKey,
          this.clientSslPassword,
          this.sslMode,
          this.rsaPublicKey,
          this.cachingRsaPublicKey,
          this.allowPublicKeyRetrieval,
          this.useServerPrepStmts,
          this.prepareCacheSize);
    }

    /**
     * Configures the connection timeout. Default unconfigured.
     *
     * @param connectTimeout the connection timeout
     * @return this {@link Builder}
     */
    public Builder connectTimeout(@Nullable Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder connectionAttributes(@Nullable Map<String, String> connectionAttributes) {
      this.connectionAttributes = connectionAttributes;
      return this;
    }

    public Builder sessionVariables(@Nullable Map<String, String> sessionVariables) {
      this.sessionVariables = sessionVariables;
      return this;
    }

    /**
     * Configure the database.
     *
     * @param database the database
     * @return this {@link Builder}
     */
    public Builder database(@Nullable String database) {
      this.database = database;
      return this;
    }

    /**
     * Configure the host.
     *
     * @param host the host
     * @return this {@link Builder}
     * @throws IllegalArgumentException if {@code host} is {@code null}
     */
    public Builder host(String host) {
      this.host = Assert.requireNonNull(host, "host must not be null");
      return this;
    }

    public Builder options(Map<String, String> options) {
      Assert.requireNonNull(options, "options map must not be null");

      options.forEach(
          (k, v) -> {
            Assert.requireNonNull(k, "option keys must not be null");
            Assert.requireNonNull(v, "option values must not be null");
          });

      this.connectionAttributes = options;
      return this;
    }

    /**
     * Configure the password.
     *
     * @param password the password
     * @return this {@link Builder}
     */
    public Builder password(@Nullable CharSequence password) {
      this.password = password;
      return this;
    }

    /**
     * Set protocol to a specific set of TLS version
     *
     * @param tlsProtocol Strings listing possible protocol, like "TLSv1.2"
     * @return this {@link Builder}
     */
    public Builder tlsProtocol(String... tlsProtocol) {
      if (tlsProtocol == null) {
        this.tlsProtocol = null;
        return this;
      }
      this.tlsProtocol = new ArrayList<>();
      for (String protocol : tlsProtocol) {
        this.tlsProtocol.add(protocol);
      }
      return this;
    }

    /**
     * Permits providing server's certificate in DER form, or server's CA certificate. The server
     * will be added to trustStore. This permits a self-signed certificate to be trusted.
     *
     * <p>Can be used in one of 3 forms :
     *
     * <ul>
     *   <li>serverSslCert=/path/to/cert.pem (full path to certificate)
     *   <li>serverSslCert=classpath:relative/cert.pem (relative to current classpath)
     *   <li>or as verbatim DER-encoded certificate string \"------BEGIN CERTIFICATE-----\" .".
     * </ul>
     *
     * @param serverSslCert certificate
     * @return this {@link Builder}
     */
    public Builder serverSslCert(String serverSslCert) {
      this.serverSslCert = serverSslCert;
      return this;
    }

    /**
     * Prepare result cache size.
     *
     * <ul>
     *   <li>0 = no cache
     *   <li>null = use default size
     *   <li>other indicate cache size
     * </ul>
     *
     * @param prepareCacheSize prepare cache size
     * @return this {@link Builder}
     */
    public Builder prepareCacheSize(Integer prepareCacheSize) {
      this.prepareCacheSize = prepareCacheSize;
      return this;
    }

    /**
     * Permits providing client's certificate for mutual authentication
     *
     * <p>Can be used in one of 3 forms :
     *
     * <ul>
     *   <li>clientSslCert=/path/to/cert.pem (full path to certificate)
     *   <li>clientSslCert=classpath:relative/cert.pem (relative to current classpath)
     *   <li>or as verbatim DER-encoded certificate string \"------BEGIN CERTIFICATE-----\" .".
     * </ul>
     *
     * @param clientSslCert certificate
     * @return this {@link Builder}
     */
    public Builder clientSslCert(String clientSslCert) {
      this.clientSslCert = clientSslCert;
      return this;
    }

    /**
     * Client private key (PKCS#8 private key file in PEM format)
     *
     * @param clientSslKey Client Private key path.
     * @return this {@link Builder}
     */
    public Builder clientSslKey(String clientSslKey) {
      this.clientSslKey = clientSslKey;
      return this;
    }

    /**
     * Client private key password if any. null if no password.
     *
     * @param clientSslPassword client private key password
     * @return this {@link Builder}
     */
    public Builder clientSslPassword(CharSequence clientSslPassword) {
      this.clientSslPassword = clientSslPassword;
      return this;
    }

    public Builder sslMode(SslMode sslMode) {
      this.sslMode = sslMode;
      if (sslMode == null) this.sslMode = SslMode.DISABLED;
      return this;
    }

    /**
     * Indicate path to MySQL server RSA public key
     *
     * @param rsaPublicKey path
     * @return this {@link Builder}
     */
    public Builder rsaPublicKey(String rsaPublicKey) {
      this.rsaPublicKey = rsaPublicKey;
      return this;
    }

    /**
     * Indicate path to MySQL server caching RSA public key
     *
     * @param cachingRsaPublicKey path
     * @return this {@link Builder}
     */
    public Builder cachingRsaPublicKey(String cachingRsaPublicKey) {
      this.cachingRsaPublicKey = cachingRsaPublicKey;
      return this;
    }

    /**
     * Permit to get MySQL server key retrieval.
     *
     * @param allowPublicKeyRetrieval indicate if permit
     * @return this {@link Builder}
     */
    public Builder allowPublicKeyRetrieval(boolean allowPublicKeyRetrieval) {
      this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
      return this;
    }

    /**
     * Permit to indicate to use text or binary protocol.
     *
     * @param useServerPrepStmts use server param
     * @return this {@link Builder}
     */
    public Builder useServerPrepStmts(boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    /**
     * Permit pipelining (sending request before resolution of previous one).
     *
     * @param allowPipelining indicate if pipelining is permit
     * @return this {@link Builder}
     */
    public Builder allowPipelining(boolean allowPipelining) {
      this.allowPipelining = allowPipelining;
      return this;
    }

    /**
     * Configure the port. Defaults to {@code 3306}.
     *
     * @param port the port
     * @return this {@link Builder}
     */
    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /**
     * Configure if multi-queries are allowed. Defaults to {@code false}.
     *
     * @param allowMultiQueries are multi-queries allowed
     * @return this {@link Builder}
     */
    public Builder allowMultiQueries(boolean allowMultiQueries) {
      this.allowMultiQueries = allowMultiQueries;
      return this;
    }

    /**
     * Configure the unix domain socket to connect to.
     *
     * @param socket the socket path
     * @return this {@link Builder}
     * @throws IllegalArgumentException if {@code socket} is {@code null}
     */
    public Builder socket(String socket) {
      this.socket = Assert.requireNonNull(socket, "host must not be null");
      return this;
    }

    public Builder username(String username) {
      this.username = Assert.requireNonNull(username, "username must not be null");
      return this;
    }

    @Override
    public Builder clone() throws CloneNotSupportedException {
      return (Builder) super.clone();
    }

    @Override
    public String toString() {
      StringBuilder hiddenPwd = new StringBuilder();
      if (password != null) {
        for (int i = 0; i < password.length(); i++) {
          hiddenPwd.append("*");
        }
      }
      return "Builder{"
          + "username='"
          + username
          + '\''
          + ", connectTimeout="
          + connectTimeout
          + ", database='"
          + database
          + '\''
          + ", host='"
          + host
          + '\''
          + ", options="
          + connectionAttributes
          + ", password="
          + hiddenPwd.toString()
          + ", port="
          + port
          + ", socket='"
          + socket
          + '\''
          + '}';
    }
  }
}
