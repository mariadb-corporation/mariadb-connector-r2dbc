package org.mariadb.r2dbc.unit;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.SslMode;
import reactor.netty.resources.LoopResources;

public class MariadbConnectionConfigurationTest {
  @Test
  public void builder() {
    TreeMap<String, String> connectionAttributes = new TreeMap<>();
    connectionAttributes.put("entry1", "val1");
    connectionAttributes.put("entry2", "val2");

    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .connectTimeout(Duration.ofMillis(150))
            .haMode("LOADBALANCE")
            .restrictedAuth("mysql_native_password,client_ed25519")
            .tcpKeepAlive(true)
            .tcpAbortiveClose(true)
            .transactionReplay(true)
            .connectionAttributes(connectionAttributes)
            .sessionVariables(Map.of("timezone", "Europe/Paris"))
            .pamOtherPwd(new String[] {"otherPwd"})
            .database("MyDB")
            .password("MyPassword")
            .tlsProtocol("TLSv1.2", "TLSv1.3")
            .serverSslCert("/path/to/serverCert")
            .prepareCacheSize(125)
            .clientSslKey("clientSecretKey")
            .clientSslPassword("ClientSecretPwd")
            .sslMode(SslMode.TRUST)
            .rsaPublicKey("/path/to/publicRSAKey")
            .cachingRsaPublicKey("cachingRSAPublicKey")
            .allowPublicKeyRetrieval(true)
            .useServerPrepStmts(true)
            .isolationLevel(IsolationLevel.SERIALIZABLE)
            .autocommit(false)
            .tinyInt1isBit(false)
            .allowPipelining(false)
            .allowMultiQueries(true)
            .socket("/path/to/mysocket")
            .username("MyUSer")
            .loopResources(LoopResources.create("mariadb"))
            .sslContextBuilderCustomizer((b) -> b)
            .sslTunnelDisableHostVerification(true)
            .build();
    Assertions.assertEquals(
        "MariadbConnectionConfiguration{database='MyDB', haMode=LOADBALANCE, hosts={[localhost:3306]}, connectTimeout=PT0.15S, tcpKeepAlive=true, tcpAbortiveClose=true, transactionReplay=true, password=*, prepareCacheSize=125, socket='/path/to/mysocket', username='MyUSer', allowMultiQueries=true, allowPipelining=false, connectionAttributes={entry1=val1, entry2=val2}, sessionVariables={timezone=Europe/Paris}, sslConfig=SslConfig{sslMode=TRUST, serverSslCert=/path/to/serverCert, clientSslCert=null, tlsProtocol=[TLSv1.2, TLSv1.3], clientSslKey=clientSecretKey}, rsaPublicKey='/path/to/publicRSAKey', cachingRsaPublicKey='cachingRSAPublicKey', allowPublicKeyRetrieval=true, isolationLevel=IsolationLevel{sql='SERIALIZABLE'}, useServerPrepStmts=false, autocommit=false, tinyInt1isBit=false, pamOtherPwd=*, restrictedAuth=[mysql_native_password, client_ed25519]}",
        conf.toString());
  }

  @Test
  public void connectionString() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:mariadb://ro%3Aot:pw%3Ad@localhost:3306/db?connectTimeout=PT0.15S"
                + "&haMode=LOADBALANCE"
                + "&restrictedAuth=mysql_native_password,client_ed25519"
                + "&tcpKeepAlive=true"
                + "&tcpAbortiveClose=true"
                + "&transactionReplay=true"
                + "&connectionAttributes=entry1=val1,entry2=val2"
                + "&sessionVariables=timezone=Europe/Paris"
                + "&pamOtherPwd=otherPwd"
                + "&tlsProtocol=TLSv1.2,TLSv1.3"
                + "&serverSslCert=/path/to/serverCert"
                + "&prepareCacheSize=125"
                + "&clientSslKey=clientSecretKey"
                + "&clientSslPassword=ClientSecretPwd"
                + "&sslMode=TRUST"
                + "&rsaPublicKey=/path/to/publicRSAKey"
                + "&cachingRsaPublicKey=cachingRSAPublicKey"
                + "&allowPublicKeyRetrieval=true"
                + "&useServerPrepStmts=true"
                + "&isolationLevel=SERIALIZABLE"
                + "&autocommit=false"
                + "&tinyInt1isBit=false"
                + "&allowPipelining=false"
                + "&allowMultiQueries=true"
                + "&socket=/path/to/mysocket"
                + "&sslTunnelDisableHostVerification=true");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals(
        "MariadbConnectionConfiguration{database='db', haMode=LOADBALANCE, hosts={[localhost:3306]}, connectTimeout=PT0.15S, tcpKeepAlive=true, tcpAbortiveClose=true, transactionReplay=true, password=*, prepareCacheSize=125, socket='/path/to/mysocket', username='ro:ot', allowMultiQueries=true, allowPipelining=false, connectionAttributes={entry1=val1, entry2=val2}, sessionVariables={timezone=Europe/Paris}, sslConfig=SslConfig{sslMode=TRUST, serverSslCert=/path/to/serverCert, clientSslCert=null, tlsProtocol=[TLSv1.2, TLSv1.3], clientSslKey=clientSecretKey}, rsaPublicKey='/path/to/publicRSAKey', cachingRsaPublicKey='cachingRSAPublicKey', allowPublicKeyRetrieval=true, isolationLevel=IsolationLevel{sql='SERIALIZABLE'}, useServerPrepStmts=false, autocommit=false, tinyInt1isBit=false, pamOtherPwd=*, restrictedAuth=[mysql_native_password, client_ed25519]}",
        conf.toString());
  }

  @Test
  public void connectionStringLoadBalance() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:mariadb:loadbalancing://ro%3Aot:pw%3Ad@localhost:3306/db?connectTimeout=PT0.15S"
                + "&restrictedAuth=mysql_native_password,client_ed25519"
                + "&tcpKeepAlive=true"
                + "&tcpAbortiveClose=true"
                + "&transactionReplay=true"
                + "&connectionAttributes=entry1=val1,entry2=val2"
                + "&sessionVariables=timezone=Europe/Paris"
                + "&pamOtherPwd=otherPwd"
                + "&tlsProtocol=TLSv1.2,TLSv1.3"
                + "&serverSslCert=/path/to/serverCert"
                + "&prepareCacheSize=125"
                + "&clientSslKey=clientSecretKey"
                + "&clientSslPassword=ClientSecretPwd"
                + "&sslMode=TRUST"
                + "&rsaPublicKey=/path/to/publicRSAKey"
                + "&cachingRsaPublicKey=cachingRSAPublicKey"
                + "&allowPublicKeyRetrieval=true"
                + "&useServerPrepStmts=true"
                + "&isolationLevel=SERIALIZABLE"
                + "&autocommit=false"
                + "&tinyInt1isBit=false"
                + "&allowPipelining=false"
                + "&allowMultiQueries=true"
                + "&socket=/path/to/mysocket"
                + "&sslTunnelDisableHostVerification=true");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals(
        "MariadbConnectionConfiguration{database='db', haMode=LOADBALANCE, hosts={[localhost:3306]}, connectTimeout=PT0.15S, tcpKeepAlive=true, tcpAbortiveClose=true, transactionReplay=true, password=*, prepareCacheSize=125, socket='/path/to/mysocket', username='ro:ot', allowMultiQueries=true, allowPipelining=false, connectionAttributes={entry1=val1, entry2=val2}, sessionVariables={timezone=Europe/Paris}, sslConfig=SslConfig{sslMode=TRUST, serverSslCert=/path/to/serverCert, clientSslCert=null, tlsProtocol=[TLSv1.2, TLSv1.3], clientSslKey=clientSecretKey}, rsaPublicKey='/path/to/publicRSAKey', cachingRsaPublicKey='cachingRSAPublicKey', allowPublicKeyRetrieval=true, isolationLevel=IsolationLevel{sql='SERIALIZABLE'}, useServerPrepStmts=false, autocommit=false, tinyInt1isBit=false, pamOtherPwd=*, restrictedAuth=[mysql_native_password, client_ed25519]}",
        conf.toString());
  }
}
