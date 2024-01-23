module r2dbc.mariadb {
    requires transitive r2dbc.spi;
    requires transitive reactor.core;
    requires transitive io.netty.buffer;
    requires transitive io.netty.handler;
    requires transitive io.netty.transport.unix.common;
    requires transitive io.netty.common;
    requires transitive io.netty.transport;
    requires transitive io.netty.codec;
    requires transitive org.reactivestreams;
    requires transitive reactor.netty.core;
    requires transitive java.naming;

    exports org.mariadb.r2dbc;
    exports org.mariadb.r2dbc.api;
    exports org.mariadb.r2dbc.authentication;
    exports org.mariadb.r2dbc.message;

    uses org.mariadb.r2dbc.authentication.AuthenticationPlugin;
    uses io.r2dbc.spi.ConnectionFactoryProvider;

    provides io.r2dbc.spi.ConnectionFactoryProvider with
            org.mariadb.r2dbc.MariadbConnectionFactoryProvider;
    provides org.mariadb.r2dbc.authentication.AuthenticationPlugin with
            org.mariadb.r2dbc.authentication.standard.NativePasswordPluginFlow,
            org.mariadb.r2dbc.authentication.addon.ClearPasswordPluginFlow,
            org.mariadb.r2dbc.authentication.standard.Ed25519PasswordPluginFlow,
            org.mariadb.r2dbc.authentication.standard.Sha256PasswordPluginFlow,
            org.mariadb.r2dbc.authentication.standard.CachingSha2PasswordFlow,
            org.mariadb.r2dbc.authentication.standard.PamPluginFlow;
}
