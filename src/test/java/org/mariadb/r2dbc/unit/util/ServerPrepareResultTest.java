package org.mariadb.r2dbc.unit.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.ServerPrepareResult;

public class ServerPrepareResultTest {
  @Test
  public void equalsTest() throws Exception {
    ServerPrepareResult prepare = new ServerPrepareResult(1, 2, new ColumnDefinitionPacket[0]);
    Assertions.assertEquals(prepare, prepare);
    Assertions.assertEquals(prepare, new ServerPrepareResult(1, 2, new ColumnDefinitionPacket[0]));
    Assertions.assertNotEquals(
        prepare, new ServerPrepareResult(2, 2, new ColumnDefinitionPacket[0]));
    Assertions.assertEquals(32, prepare.hashCode());
    Assertions.assertFalse(prepare.equals(null));
    Assertions.assertFalse(prepare.equals("dd"));
  }
}
