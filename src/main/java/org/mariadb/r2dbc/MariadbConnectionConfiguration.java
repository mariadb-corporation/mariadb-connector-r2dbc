// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.Security;
import org.mariadb.r2dbc.util.SslConfig;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;
import reactor.util.annotation.Nullable;

public final class MariadbConnectionConfiguration {

  public static final int DEFAULT_PORT = 3306;
  private final String database;
  private final List<HostAddress> hostAddresses;
  private final HaMode haMode;
  private final Duration connectTimeout;
  private final boolean tcpKeepAlive;
  private final boolean tcpAbortiveClose;
  private final boolean transactionReplay;
  private final CharSequence password;
  private final String collation;
  private final CharSequence[] pamOtherPwd;
  private final int port;
  private final int prepareCacheSize;
  private final String socket;
  private final String username;
  private final boolean allowMultiQueries;
  private final boolean allowPipelining;
  private final Map<String, String> connectionAttributes;
  private final Map<String, Object> sessionVariables;
  private final SslConfig sslConfig;
  private final String rsaPublicKey;
  private final String cachingRsaPublicKey;
  private final boolean allowPublicKeyRetrieval;
  private IsolationLevel isolationLevel;
  private final boolean useServerPrepStmts;
  private final boolean autocommit;
  private final boolean tinyInt1isBit;
  private final boolean useBulkStmts;
  private final boolean useBulkStmtsForInserts;
  private final String[] restrictedAuth;
  private final LoopResources loopResources;
  private final UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer;

  private MariadbConnectionConfiguration(
      String haMode,
      @Nullable Duration connectTimeout,
      @Nullable Boolean tcpKeepAlive,
      @Nullable Boolean tcpAbortiveClose,
      @Nullable Boolean transactionReplay,
      @Nullable String database,
      @Nullable String host,
      @Nullable Map<String, String> connectionAttributes,
      @Nullable Map<String, Object> sessionVariables,
      @Nullable CharSequence password,
      @Nullable String collation,
      int port,
      @Nullable List<HostAddress> hostAddresses,
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
      IsolationLevel isolationLevel,
      boolean autocommit,
      @Nullable Integer prepareCacheSize,
      @Nullable CharSequence[] pamOtherPwd,
      boolean tinyInt1isBit,
      boolean useBulkStmts,
      boolean useBulkStmtsForInserts,
      String restrictedAuth,
      @Nullable LoopResources loopResources,
      @Nullable UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer,
      boolean sslTunnelDisableHostVerification) {
    this.haMode = haMode == null ? HaMode.NONE : HaMode.from(haMode);
    this.connectTimeout = connectTimeout == null ? Duration.ofSeconds(10) : connectTimeout;
    this.tcpKeepAlive = tcpKeepAlive == null ? Boolean.FALSE : tcpKeepAlive;
    this.tcpAbortiveClose = tcpAbortiveClose == null ? Boolean.FALSE : tcpAbortiveClose;
    this.transactionReplay = transactionReplay == null ? Boolean.FALSE : transactionReplay;
    this.database = database != null && !database.isEmpty() ? database : null;
    this.isolationLevel = isolationLevel;
    this.restrictedAuth = restrictedAuth != null ? restrictedAuth.split(",") : null;
    if (hostAddresses != null) {
      this.hostAddresses = hostAddresses;
    } else {
      this.hostAddresses = HostAddress.parse(host, port);
    }
    this.connectionAttributes = connectionAttributes;
    this.sessionVariables = sessionVariables;
    this.password = password != null && !password.toString().isEmpty() ? password : null;
    this.collation = collation;
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
              sslMode,
              serverSslCert,
              clientSslCert,
              clientSslKey,
              clientSslPassword,
              tlsProtocol,
              sslTunnelDisableHostVerification,
              sslContextBuilderCustomizer);
    }
    this.rsaPublicKey = rsaPublicKey;
    this.cachingRsaPublicKey = cachingRsaPublicKey;
    this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
    this.prepareCacheSize = (prepareCacheSize == null) ? 250 : prepareCacheSize.intValue();
    this.pamOtherPwd = pamOtherPwd;
    this.autocommit = autocommit;
    this.tinyInt1isBit = tinyInt1isBit;
    this.useBulkStmts = useBulkStmts;
    this.useBulkStmtsForInserts = useBulkStmtsForInserts;
    this.loopResources = loopResources != null ? loopResources : TcpResources.get();
    this.useServerPrepStmts = !this.allowMultiQueries && useServerPrepStmts;
    this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
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

  @SuppressWarnings("unchecked")
  public static Builder fromOptions(ConnectionFactoryOptions connectionFactoryOptions) {
    Builder builder = new Builder();
    builder.database((String) connectionFactoryOptions.getValue(DATABASE));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SOCKET)) {
      builder.socket(
          (String)
              connectionFactoryOptions.getRequiredValue(MariadbConnectionFactoryProvider.SOCKET));
    } else {
      builder.host((String) connectionFactoryOptions.getRequiredValue(HOST));
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

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TCP_KEEP_ALIVE)) {
      builder.tcpKeepAlive(
          boolValue(
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.TCP_KEEP_ALIVE)));
    }

    if (connectionFactoryOptions.hasOption(
        MariadbConnectionFactoryProvider.ALLOW_PUBLIC_KEY_RETRIEVAL)) {
      builder.allowPublicKeyRetrieval(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.ALLOW_PUBLIC_KEY_RETRIEVAL)));
    }

    if (connectionFactoryOptions.hasOption(
        MariadbConnectionFactoryProvider.CACHING_RSA_PUBLIC_KEY)) {
      builder.cachingRsaPublicKey(
          (String)
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.CACHING_RSA_PUBLIC_KEY));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.RSA_PUBLIC_KEY)) {
      builder.rsaPublicKey(
          (String)
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.RSA_PUBLIC_KEY));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TCP_ABORTIVE_CLOSE)) {
      builder.tcpAbortiveClose(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.TCP_ABORTIVE_CLOSE)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TRANSACTION_REPLAY)) {
      builder.transactionReplay(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.TRANSACTION_REPLAY)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.SESSION_VARIABLES)) {
      String sessionVarString =
          (String)
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SESSION_VARIABLES);
      builder.sessionVariables(Security.parseSessionVariables(sessionVarString));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.HAMODE)) {
      String haMode =
          (String) connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.HAMODE);
      builder.haMode(haMode);
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
    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.ISOLATION_LEVEL)) {
      String isolationLvl =
          (String)
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.ISOLATION_LEVEL);
      builder.isolationLevel(
          isolationLvl == null ? null : IsolationLevel.valueOf(isolationLvl.replace("-", " ")));
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

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.USE_BULK_STMT)) {
      builder.useBulkStmts(
              boolValue(
                      connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.USE_BULK_STMT)));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.USE_BULK_STMT_FOR_INSERTS)) {
      builder.useBulkStmtsForInserts(
              boolValue(
                      connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.USE_BULK_STMT_FOR_INSERTS)));
    }

    if (connectionFactoryOptions.hasOption(
        MariadbConnectionFactoryProvider.CONNECTION_ATTRIBUTES)) {
      String connAttributes =
          (String)
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.CONNECTION_ATTRIBUTES);
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
              (String)
                  connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SSL_MODE)));
    }
    builder.serverSslCert(
        (String)
            connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.SERVER_SSL_CERT));
    builder.clientSslCert(
        (String)
            connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_CERT));
    builder.clientSslKey(
        (String)
            connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_KEY));
    builder.clientSslPassword(
        (String)
            connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.CLIENT_SSL_PWD));

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.TLS_PROTOCOL)) {
      String[] protocols =
          ((String)
                  connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.TLS_PROTOCOL))
              .split("[,;\\s]+");
      builder.tlsProtocol(protocols);
    }
    if (connectionFactoryOptions.hasOption(ConnectionFactoryOptions.PROTOCOL)) {
      builder.haMode((String) connectionFactoryOptions.getValue(ConnectionFactoryOptions.PROTOCOL));
    }
    builder.password((CharSequence) connectionFactoryOptions.getValue(PASSWORD));
    builder.collation(
        (String) connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.COLLATION));

    builder.username((String) connectionFactoryOptions.getRequiredValue(USER));
    if (connectionFactoryOptions.hasOption(PORT)) {
      builder.port(intValue(connectionFactoryOptions.getValue(PORT)));
    }
    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.PAM_OTHER_PASSWORD)) {
      String s =
          (String)
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.PAM_OTHER_PASSWORD);
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

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.RESTRICTED_AUTH)) {
      builder.restrictedAuth(
          (String)
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.RESTRICTED_AUTH));
    }

    if (connectionFactoryOptions.hasOption(MariadbConnectionFactoryProvider.LOOP_RESOURCES)) {
      LoopResources loopResources =
          (LoopResources)
              connectionFactoryOptions.getValue(MariadbConnectionFactoryProvider.LOOP_RESOURCES);
      builder.loopResources(loopResources);
    }

    if (connectionFactoryOptions.hasOption(
        MariadbConnectionFactoryProvider.SSL_TUNNEL_DISABLE_HOST_VERIFICATION)) {
      builder.sslTunnelDisableHostVerification(
          boolValue(
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.SSL_TUNNEL_DISABLE_HOST_VERIFICATION)));
    }

    if (connectionFactoryOptions.hasOption(
        MariadbConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER)) {
      builder.sslContextBuilderCustomizer(
          (UnaryOperator<SslContextBuilder>)
              connectionFactoryOptions.getValue(
                  MariadbConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER));
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

  private void setIsolationLevel(IsolationLevel isolationLevel) {
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

  public HaMode getHaMode() {
    return this.haMode;
  }

  @Nullable
  public List<HostAddress> getHostAddresses() {
    return this.hostAddresses;
  }

  @Nullable
  public Map<String, String> getConnectionAttributes() {
    return this.connectionAttributes;
  }

  @Nullable
  public Map<String, Object> getSessionVariables() {
    return this.sessionVariables;
  }

  @Nullable
  public CharSequence getPassword() {
    return this.password;
  }

  @Nullable
  public String getCollation() {
    return this.collation;
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
  public boolean useBulkStmts() {
    return useBulkStmts;
  }

  public boolean useBulkStmtsForInserts() {
    return useBulkStmtsForInserts;
  }
  public int getPrepareCacheSize() {
    return prepareCacheSize;
  }

  public boolean isTcpKeepAlive() {
    return tcpKeepAlive;
  }

  public boolean isTcpAbortiveClose() {
    return tcpAbortiveClose;
  }

  public boolean isTransactionReplay() {
    return transactionReplay;
  }

  public String[] getRestrictedAuth() {
    return restrictedAuth;
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
        + ", haMode="
        + haMode
        + ", hosts={"
        + (hostAddresses == null ? "" : Arrays.toString(hostAddresses.toArray()))
        + '}'
        + ", connectTimeout="
        + connectTimeout
        + ", tcpKeepAlive="
        + tcpKeepAlive
        + ", tcpAbortiveClose="
        + tcpAbortiveClose
        + ", transactionReplay="
        + transactionReplay
        + ", password="
        + hiddenPwd
        + ", prepareCacheSize="
        + prepareCacheSize
        + ", socket='"
        + socket
        + '\''
        + ", username='"
        + username
        + '\''
        + ", collation='"
        + collation
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
            + ", useBulkStmts="
            + useBulkStmts
            + ", useBulkStmtsForInserts="
            + useBulkStmtsForInserts
        + ", pamOtherPwd="
        + hiddenPamPwd
        + ", restrictedAuth="
        + (restrictedAuth == null ? "" : Arrays.toString(restrictedAuth))
        + '}';
  }

  /**
   * A builder for {@link MariadbConnectionConfiguration} instances.
   *
   * <p><i>This class is not threadsafe</i>
   */
  public static final class Builder implements Cloneable {

    @Nullable private String haMode;
    @Nullable private String rsaPublicKey;
    @Nullable private String cachingRsaPublicKey;
    private boolean allowPublicKeyRetrieval;
    @Nullable private String username;
    @Nullable private Duration connectTimeout;
    @Nullable private Boolean tcpKeepAlive;
    @Nullable private Boolean tcpAbortiveClose;
    @Nullable private Boolean transactionReplay;
    @Nullable private String database;
    @Nullable private List<HostAddress> hostAddresses;
    @Nullable private String host;
    @Nullable private Map<String, Object> sessionVariables;
    @Nullable private Map<String, String> connectionAttributes;
    @Nullable private CharSequence password;
    @Nullable private String collation;

    private int port = DEFAULT_PORT;
    @Nullable private String socket;
    private boolean allowMultiQueries = false;
    private boolean allowPipelining = true;
    private boolean useServerPrepStmts = false;
    private IsolationLevel isolationLevel = null;
    private boolean autocommit = true;
    private boolean tinyInt1isBit = true;
    private boolean useBulkStmts = false;
    private boolean useBulkStmtsForInserts = true;

    @Nullable Integer prepareCacheSize;
    @Nullable private List<String> tlsProtocol;
    @Nullable private String serverSslCert;
    @Nullable private String clientSslCert;
    @Nullable private String clientSslKey;
    @Nullable private CharSequence clientSslPassword;
    private SslMode sslMode = SslMode.DISABLE;
    private CharSequence[] pamOtherPwd;
    private String restrictedAuth;
    @Nullable private LoopResources loopResources;
    @Nullable private UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer;

    private Builder() {}

    private boolean sslTunnelDisableHostVerification;

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
          this.haMode,
          this.connectTimeout,
          this.tcpKeepAlive,
          this.tcpAbortiveClose,
          this.transactionReplay,
          this.database,
          this.host,
          this.connectionAttributes,
          this.sessionVariables,
          this.password,
          this.collation,
          this.port,
          this.hostAddresses,
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
          this.isolationLevel,
          this.autocommit,
          this.prepareCacheSize,
          this.pamOtherPwd,
          this.tinyInt1isBit,
          this.useBulkStmts,
          this.useBulkStmtsForInserts,
          this.restrictedAuth,
          this.loopResources,
          this.sslContextBuilderCustomizer,
          this.sslTunnelDisableHostVerification);
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

    public Builder haMode(@Nullable String haMode) {
      this.haMode = haMode;
      return this;
    }

    public Builder hostAddresses(@Nullable List<HostAddress> hostAddresses) {
      this.hostAddresses = hostAddresses;
      return this;
    }

    public Builder restrictedAuth(@Nullable String restrictedAuth) {
      this.restrictedAuth = restrictedAuth;
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

    public Builder transactionReplay(@Nullable Boolean transactionReplay) {
      this.transactionReplay = transactionReplay;
      return this;
    }

    public Builder connectionAttributes(@Nullable Map<String, String> connectionAttributes) {
      this.connectionAttributes = connectionAttributes;
      return this;
    }

    /**
     * Set session variable
     *
     * @param sessionVariables map containing session variables
     * @return this {@link Builder}
     */
    public Builder sessionVariables(@Nullable Map<String, Object> sessionVariables) {
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
     * Configure the utf8mb4 collation.
     *
     * @param collation default utf8mb4 collation if not using server default collation
     * @return this {@link Builder}
     */
    public Builder collation(@Nullable String collation) {
      this.collation = collation;
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
     * Permit to set default isolation level
     *
     * @param isolationLevel transaction isolation level
     * @return this {@link Builder}
     */
    public Builder isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
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
     * Use dedicated COM_STMT_BULK_EXECUTE protocol for batch insert when possible to have faster batch.
     * (significant only on >= MariaDB 10.2.7). Default: false.
     *
     * @param useBulkStmts return bulk option
     * @return this {@link Builder}
     */
    public Builder useBulkStmts(boolean useBulkStmts) {
      this.useBulkStmts = useBulkStmts;
      return this;
    }

    /**
     * use Bulk batch for insert only.
     * This permits to ensure returning expected affected rows.
     * This option is enabled when useBulkStmts is enabled
     *
     * @param useBulkStmtsForInserts return bulk option for insert commands
     * @return this {@link Builder}
     */
    public Builder useBulkStmtsForInserts(boolean useBulkStmtsForInserts) {
      this.useBulkStmtsForInserts = useBulkStmtsForInserts;
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

    public Builder sslContextBuilderCustomizer(
        UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer) {
      this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
      return this;
    }

    public Builder sslTunnelDisableHostVerification(boolean sslTunnelDisableHostVerification) {
      this.sslTunnelDisableHostVerification = sslTunnelDisableHostVerification;
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
          + ", haMode="
          + haMode
          + ", cachingRsaPublicKey="
          + cachingRsaPublicKey
          + ", allowPublicKeyRetrieval="
          + allowPublicKeyRetrieval
          + ", username="
          + username
          + ", connectTimeout="
          + connectTimeout
          + ", tcpKeepAlive="
          + tcpKeepAlive
          + ", tcpAbortiveClose="
          + tcpAbortiveClose
          + ", transactionReplay="
          + transactionReplay
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
          + ", restrictedAuth="
          + restrictedAuth
          + ", port="
          + port
          + ", hosts={"
          + (hostAddresses == null ? "" : Arrays.toString(hostAddresses.toArray()))
          + '}'
          + ", socket="
          + socket
          + ", allowMultiQueries="
          + allowMultiQueries
          + ", allowPipelining="
          + allowPipelining
          + ", useServerPrepStmts="
          + useServerPrepStmts
          + ", collation="
          + collation
          + ", prepareCacheSize="
          + isolationLevel
          + ", isolationLevel="
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
          + ", sslTunnelDisableHostVerification="
          + sslTunnelDisableHostVerification
          + ", pamOtherPwd="
          + hiddenPamPwd
          + ", tinyInt1isBit="
          + tinyInt1isBit
              + ", useBulkStmts="
              + useBulkStmts

          + ", autoCommit="
          + autocommit
          + '}';
    }
  }
}
