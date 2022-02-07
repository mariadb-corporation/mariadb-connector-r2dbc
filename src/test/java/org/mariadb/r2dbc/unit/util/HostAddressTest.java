package org.mariadb.r2dbc.unit.util;

import io.r2dbc.spi.ConnectionFactoryOptions;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.util.HostAddress;

public class HostAddressTest {
  @Test
  void parseTest() {
    List<HostAddress> addresses = HostAddress.parse("host1:3303,host2:3305", 3306);
    Assertions.assertEquals(2, addresses.size());
    Assertions.assertEquals(new HostAddress("host1", 3303), addresses.get(0));
    Assertions.assertEquals(new HostAddress("host2", 3305), addresses.get(1));

    List<HostAddress> addresses2 = HostAddress.parse(null, 3303);
    Assertions.assertEquals(1, addresses2.size());
    Assertions.assertEquals(new HostAddress("localhost", 3303), addresses2.get(0));
    Assertions.assertNotEquals(addresses.hashCode(), addresses2.hashCode());
  }

  @Test
  void parseTestSpiFromOption() {
    final ConnectionFactoryOptions option1s =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.USER, "someUser")
            .option(ConnectionFactoryOptions.HOST, "host1:3303,host2,host3:3305,host4")
            .build();

    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(option1s).build();
    Assertions.assertEquals(4, conf.getHostAddresses().size());
    Assertions.assertEquals(new HostAddress("host1", 3303), conf.getHostAddresses().get(0));
    Assertions.assertEquals(new HostAddress("host2", 3306), conf.getHostAddresses().get(1));
    Assertions.assertEquals(new HostAddress("host3", 3305), conf.getHostAddresses().get(2));
    Assertions.assertEquals(new HostAddress("host4", 3306), conf.getHostAddresses().get(3));

    final ConnectionFactoryOptions option2s =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.USER, "someUser")
            .option(ConnectionFactoryOptions.HOST, "host1:3303,host2:3305")
            .option(ConnectionFactoryOptions.PORT, 3307)
            .build();

    conf = MariadbConnectionConfiguration.fromOptions(option2s).build();
    Assertions.assertEquals(2, conf.getHostAddresses().size());
    Assertions.assertEquals(new HostAddress("host1", 3303), conf.getHostAddresses().get(0));
    Assertions.assertEquals(new HostAddress("host2", 3305), conf.getHostAddresses().get(1));

    final ConnectionFactoryOptions option3s =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.USER, "someUser")
            .option(ConnectionFactoryOptions.HOST, "host1:3303,host2,host3:3309")
            .option(ConnectionFactoryOptions.PORT, 3307)
            .build();

    conf = MariadbConnectionConfiguration.fromOptions(option3s).build();
    Assertions.assertEquals(3, conf.getHostAddresses().size());
    Assertions.assertEquals(new HostAddress("host1", 3303), conf.getHostAddresses().get(0));
    Assertions.assertEquals(new HostAddress("host2", 3307), conf.getHostAddresses().get(1));
    Assertions.assertEquals(new HostAddress("host3", 3309), conf.getHostAddresses().get(2));
    Assertions.assertEquals(
        "HostAddress{host='host3', port=3309}", conf.getHostAddresses().get(2).toString());
  }

  @Test
  void parseTestSpiFromString() {

    final ConnectionFactoryOptions option1s =
        ConnectionFactoryOptions.parse(
            "r2dbc:mariadb://someUser:pwd@host1:3303,host2,host3:3305,host4/");

    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(option1s).build();
    Assertions.assertEquals(4, conf.getHostAddresses().size());
    Assertions.assertEquals(new HostAddress("host1", 3303), conf.getHostAddresses().get(0));
    Assertions.assertEquals(new HostAddress("host2", 3306), conf.getHostAddresses().get(1));
    Assertions.assertEquals(new HostAddress("host3", 3305), conf.getHostAddresses().get(2));
    Assertions.assertEquals(new HostAddress("host4", 3306), conf.getHostAddresses().get(3));

    final ConnectionFactoryOptions option2s =
        ConnectionFactoryOptions.parse("r2dbc:mariadb://someUser:pwd@host1:3303,host2:3305/");

    conf = MariadbConnectionConfiguration.fromOptions(option2s).build();
    Assertions.assertEquals(2, conf.getHostAddresses().size());
    Assertions.assertEquals(new HostAddress("host1", 3303), conf.getHostAddresses().get(0));
    Assertions.assertEquals(new HostAddress("host2", 3305), conf.getHostAddresses().get(1));

    final ConnectionFactoryOptions option3s =
        ConnectionFactoryOptions.parse("r2dbc:mariadb://someUser:pwd@host1:3303,host2,host3:3309/");
    conf = MariadbConnectionConfiguration.fromOptions(option3s).build();
    Assertions.assertEquals(3, conf.getHostAddresses().size());
    Assertions.assertEquals(new HostAddress("host1", 3303), conf.getHostAddresses().get(0));
    Assertions.assertEquals(new HostAddress("host2", 3306), conf.getHostAddresses().get(1));
    Assertions.assertEquals(new HostAddress("host3", 3309), conf.getHostAddresses().get(2));
    Assertions.assertEquals(
        "HostAddress{host='host3', port=3309}", conf.getHostAddresses().get(2).toString());
  }
}
