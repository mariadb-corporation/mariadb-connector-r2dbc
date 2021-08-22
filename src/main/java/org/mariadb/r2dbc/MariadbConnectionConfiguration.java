// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.SslConfig;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;
import reactor.util.annotation.Nullable;

public final class MariadbConnectionConfiguration {

  public static final int DEFAULT_PORT = 3306;
  private final String database;
  private final String host;

  private final Duration connectTimeout;
  private final Duration socketTimeout;
  private final boolean tcpKeepAlive;
  private final boolean tcpAbortiveClose;
  private final CharSequence password;
  private final CharSequence[] pamOtherPwd;
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
  private final boolean autocommit;
  private final boolean tinyInt1isBit;
  private final LoopResources loopResources;

  private MariadbConnectionConfiguration(
      @Nullable Duration connectTimeout,
      @Nullable Duration socketTimeout,
      @Nullable Boolean tcpKeepAlive,
      @Nullable Boolean tcpAbortiveClose,
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
      boolean autocommit,
      @Nullable Integer prepareCacheSize,
      @Nullable CharSequence[] pamOtherPwd,
      boolean tinyInt1isBit,
      @Nullable LoopResources loopResources) {
    this.connectTimeout = connectTimeout == null ? Duration.ofSeconds(10) : connectTimeout;
    this.socketTimeout = socketTimeout;
    this.tcpKeepAlive = tcpKeepAlive == null ? Boolean.FALSE : tcpKeepAlive;
    this.tcpAbortiveClose = tcpAbortiveClose == null ? Boolean.FALSE : tcpAbortiveClose;
    this.database = database != null && !database.isEmpty() ? database : null;
    this.host = host;
    this.connectionAttributes = connectionAttributes;
    this.sessionVariables = sessionVariables;
    this.password = password != null && !password.toString().isEmpty() ? password : null;
    this.port = port;
    this.socket = socket;
    this.username = username;
    this.allowMultiQueries = allowMultiQueries;
    this.allowPipelining = allowPipelining;
    if (sslMode == SslMode.DISABLE) {
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
    this.pamOtherPwd = pamOtherPwd;
    this.autocommit = autocommit;
    this.tinyInt1isBit = tinyInt1isBit;
    this.loopResources = loopResources != null ? loopResources : TcpResources.get();
  }

  static boolean boolValue(Object value) {
    if (value instanceof Boolean) {
      return ((Boolean) value).booleanValue();
    }
    return Boolean.parseBoolean(value.toString()) || "1".equals(value);
  }

  static Duration durationValue(Object value) {
    if (value instanceof Duration) {
      return ((Duration) value);
    }
    return Duration.parse(value.toString());
  }

  static int intValue(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    return Integer.parseInt(value.toString());
  }

  public static Builder fromOptions(ConnectionFactoryOptions connectionFactoryOptions) {
    Builder builder = new Builder();
    builder.database(connectionFactoryOptions.getValue(DATABASE));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SOCKET)) {
      builder.socket(
          connectionFactoryOptions.getRequiredValue(MariadbConnectionFactoryProvider.SOCKET));
    } else {
      builder.host(connectionFactoryOptions.getRequiredValue(HOST));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES)) {
      builder.allowMultiQueries(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.ALLOW_MULTI_QUERIES)));
    }

    if (connectionFactoryOptions.hasOption(ConnectionFactoryOptions.CONNECT_TIMEOUT)) {
      builder.connectTimeout(
          durationValue(
              connectionFactoryOptions.getValue(ConnectionFactoryOptions.CONNECT_TIMEOUT)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SOCKET_TIMEOUT)) {
      builder.socketTimeout(
          durationValue(
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SOCKET_TIMEOUT)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TCP_KEEP_ALIVE)) {
      builder.tcpKeepAlive(
          boolValue(
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.TCP_KEEP_ALIVE)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TCP_ABORTIVE_CLOSE)) {
      builder.tcpAbortiveClose(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.TCP_ABORTIVE_CLOSE)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SESSION_VARIABLES)) {
      String sessionVarString =
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SESSION_VARIABLES);
      builder.sessionVariables(getMapFromString(sessionVarString));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.ALLOW_PIPELINING)) {
      builder.allowPipelining(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.ALLOW_PIPELINING)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.USE_SERVER_PREPARE)) {
      builder.useServerPrepStmts(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.USE_SERVER_PREPARE)));
    }
    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.AUTO_COMMIT)) {
      builder.autocommit(
          boolValue(
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.AUTO_COMMIT)));
    }
    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TINY_IS_BIT)) {
      builder.tinyInt1isBit(
          boolValue(
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.TINY_IS_BIT)));
    }

    if (connectionFactoryOptions.hasOption(
        MariadbConnectionFactoryProvider.CONNECTION_ATTRIBUTES)) {
      String connAttributes =
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CONNECTION_ATTRIBUTES);
      builder.connectionAttributes(getMapFromString(connAttributes));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.PREPARE_CACHE_SIZE)) {
      builder.prepareCacheSize(
          intValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.PREPARE_CACHE_SIZE)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SSL_MODE)) {
      builder.sslMode(
          SslMode.from(
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SSL_MODE)));
    }
    builder.serverSslCert(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SERVER_SSL_CERT));
    builder.clientSslCert(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_CERT));
    builder.clientSslKey(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_KEY));
    builder.clientSslPassword(
        connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_PWD));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TLS_PROTOCOL)) {
      String[] protocols =
          connectionFactoryOptions
              .getValue(MariadbConnectionFactoryProvider.TLS_PROTOCOL)
              .split("[,;\\s]+");
      builder.tlsProtocol(protocols);
    }
    builder.password(connectionFactoryOptions.getValue(PASSWORD));
    builder.username(connectionFactoryOptions.getRequiredValue(USER));
    if (connectionFactoryOptions.hasOption(PORT)) {
      builder.port(intValue(connectionFactoryOptions.getValue(PORT)));
    }
    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.PAM_OTHER_PASSWORD)) {
      String s =
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.PAM_OTHER_PASSWORD);
      String[] pairs = s.split(",");
      try {
        for (int i = 0; i < pairs.length; i++) {
          pairs[i] = URLDecoder.decode(pairs[i], StandardCharsets.UTF_8.toString());
        }
      } catch (UnsupportedEncodingException e) {
        // eat, StandardCharsets.UTF_8 is always supported
      }
      builder.pamOtherPwd(pairs);
    }
    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.LOOP_RESOURCES)) {
      builder.loopResources(
          connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.LOOP_RESOURCES));
    }

    return builder;
  }

  private static Map<String, String> getMapFromString(String s) {
    Map<String, String> map = new HashMap<>();
    if (s != null && !s.isEmpty()) {
      String[] pairs = s.split(",");
      for (int i = 0; i < pairs.length; i++) {
        String pair = pairs[i];
        String[] keyValue = pair.split("=");
        map.put(keyValue[0], (keyValue.length > 1) ? keyValue[1] : "");
      }
    }
    return map;
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

  public CharSequence[] getPamOtherPwd() {
    return pamOtherPwd;
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

  public boolean autocommit() {
    return autocommit;
  }

  public boolean tinyInt1isBit() {
    return tinyInt1isBit;
  }

  public int getPrepareCacheSize() {
    return prepareCacheSize;
  }

  public Duration getSocketTimeout() {
    return socketTimeout;
  }

  public boolean isTcpKeepAlive() {
    return tcpKeepAlive;
  }

  public boolean isTcpAbortiveClose() {
    return tcpAbortiveClose;
  }

  public LoopResources loopResources() {
    return loopResources;
  }

  @Override
  public String toString() {
    StringBuilder hiddenPwd = new StringBuilder();
    if (password != null) {
      hiddenPwd.append("*");
    }
    StringBuilder hiddenPamPwd = new StringBuilder();
    if (pamOtherPwd != null) {
      for (CharSequence s : pamOtherPwd) {
        hiddenPamPwd.append("*");
        hiddenPamPwd.append(",");
      }
      hiddenPamPwd.deleteCharAt(hiddenPamPwd.length() - 1);
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
        + ", socketTimeout="
        + socketTimeout
        + ", tcpKeepAlive="
        + tcpKeepAlive
        + ", tcpAbortiveClose="
        + tcpAbortiveClose
        + ", password="
        + hiddenPwd
        + ", port="
        + port
        + ", prepareCacheSize="
        + prepareCacheSize
        + ", socket='"
        + socket
        + '\''
        + ", username='"
        + username
        + '\''
        + ", allowMultiQueries="
        + allowMultiQueries
        + ", allowPipelining="
        + allowPipelining
        + ", connectionAttributes="
        + connectionAttributes
        + ", sessionVariables="
        + sessionVariables
        + ", sslConfig="
        + sslConfig
        + ", rsaPublicKey='"
        + rsaPublicKey
        + '\''
        + ", cachingRsaPublicKey='"
        + cachingRsaPublicKey
        + '\''
        + ", allowPublicKeyRetrieval="
        + allowPublicKeyRetrieval
        + ", isolationLevel="
        + isolationLevel
        + ", useServerPrepStmts="
        + useServerPrepStmts
        + ", autocommit="
        + autocommit
        + ", tinyInt1isBit="
        + tinyInt1isBit
        + ", pamOtherPwd="
        + hiddenPamPwd
        + '}';
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
    @Nullable private Duration socketTimeout;
    @Nullable private Boolean tcpKeepAlive;
    @Nullable private Boolean tcpAbortiveClose;
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
    private boolean autocommit = true;
    private boolean tinyInt1isBit = true;
    @Nullable Integer prepareCacheSize;
    @Nullable private List<String> tlsProtocol;
    @Nullable private String serverSslCert;
    @Nullable private String clientSslCert;
    @Nullable private String clientSslKey;
    @Nullable private CharSequence clientSslPassword;
    private SslMode sslMode = SslMode.DISABLE;
    private CharSequence[] pamOtherPwd;
    @Nullable private LoopResources loopResources;

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
          this.socketTimeout,
          this.tcpKeepAlive,
          this.tcpAbortiveClose,
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
          this.autocommit,
          this.prepareCacheSize,
          this.pamOtherPwd,
          this.tinyInt1isBit,
          this.loopResources);
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

    public Builder socketTimeout(@Nullable Duration socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public Builder tcpKeepAlive(@Nullable Boolean tcpKeepAlive) {
      this.tcpKeepAlive = tcpKeepAlive;
      return this;
    }

    public Builder tcpAbortiveClose(@Nullable Boolean tcpAbortiveClose) {
      this.tcpAbortiveClose = tcpAbortiveClose;
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

    public Builder pamOtherPwd(@Nullable CharSequence[] pamOtherPwd) {
      this.pamOtherPwd = pamOtherPwd;
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
      List<String> tmp = new ArrayList<>();
      for (String protocol : tlsProtocol) {
        if (protocol != null) tmp.add(protocol);
      }
      if (!tmp.isEmpty()) this.tlsProtocol = tmp;
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
      if (sslMode == null) this.sslMode = SslMode.DISABLE;
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
     * Permit to indicate default autocommit value. Default value True.
     *
     * @param autocommit use autocommit
     * @return this {@link Builder}
     */
    public Builder autocommit(boolean autocommit) {
      this.autocommit = autocommit;
      return this;
    }

    /**
     * Permit to indicate how BIT(1) must return as boolean or byte . Default value True (returns
     * boolean).
     *
     * @param tinyInt1isBit return boolean for BIT(1)
     * @return this {@link Builder}
     */
    public Builder tinyInt1isBit(boolean tinyInt1isBit) {
      this.tinyInt1isBit = tinyInt1isBit;
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

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = Assert.requireNonNull(loopResources, "loopResources must not be null");
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
        hiddenPwd.append("*");
      }
      StringBuilder hiddenPamPwd = new StringBuilder();
      if (pamOtherPwd != null) {
        for (CharSequence s : pamOtherPwd) {
          hiddenPamPwd.append("*");
          hiddenPamPwd.append(",");
        }
        hiddenPamPwd.deleteCharAt(hiddenPamPwd.length() - 1);
      }

      return "Builder{"
          + "rsaPublicKey="
          + rsaPublicKey
          + ", cachingRsaPublicKey="
          + cachingRsaPublicKey
          + ", allowPublicKeyRetrieval="
          + allowPublicKeyRetrieval
          + ", username="
          + username
          + ", connectTimeout="
          + connectTimeout
          + ", socketTimeout="
          + socketTimeout
          + ", tcpKeepAlive="
          + tcpKeepAlive
          + ", tcpAbortiveClose="
          + tcpAbortiveClose
          + ", database="
          + database
          + ", host="
          + host
          + ", sessionVariables="
          + sessionVariables
          + ", connectionAttributes="
          + connectionAttributes
          + ", password="
          + hiddenPwd
          + ", port="
          + port
          + ", socket="
          + socket
          + ", allowMultiQueries="
          + allowMultiQueries
          + ", allowPipelining="
          + allowPipelining
          + ", useServerPrepStmts="
          + useServerPrepStmts
          + ", prepareCacheSize="
          + prepareCacheSize
          + ", tlsProtocol="
          + tlsProtocol
          + ", serverSslCert="
          + serverSslCert
          + ", clientSslCert="
          + clientSslCert
          + ", clientSslKey="
          + clientSslKey
          + ", clientSslPassword="
          + clientSslPassword
          + ", sslMode="
          + sslMode
          + ", pamOtherPwd="
          + hiddenPamPwd
          + ", tinyInt1isBit="
          + tinyInt1isBit
          + ", autoCommit="
          + autocommit
          + '}';
    }
  }
}
