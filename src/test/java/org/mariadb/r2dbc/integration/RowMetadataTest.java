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

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import reactor.test.StepVerifier;

public class RowMetadataTest extends BaseTest {

  @BeforeAll
  public static void before2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS rowMeta").execute().blockLast();
    sharedConn
        .createStatement(
            "CREATE TABLE rowMeta (t1 varchar(256) NOT NULL, t2 Integer, t3 DECIMAL(10,6), t4 DECIMAL(10,6) unsigned)"
                + " CHARACTER SET "
                + "utf8mb4 "
                + "COLLATE "
                + "utf8mb4_unicode_ci")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO rowMeta VALUES ('some🌟', 1, 2, 2),('1', 2, 10, 11),('0', 3, 100, 101), ('3', null, null, "
                + "null)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS rowMeta").execute().blockLast();
  }

  @Test
  void rowMeta() {
    sharedConn
        .createStatement("SELECT * FROM rowMeta WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      List<String> expected = Arrays.asList("t1", "t2", "t3", "t4");
                      assertEquals(expected.size(), metadata.getColumnNames().size());
                      assertArrayEquals(expected.toArray(), metadata.getColumnNames().toArray());
                      this.assertThrows(
                          IllegalArgumentException.class,
                          () -> metadata.getColumnMetadata(-1),
                          "Column index -1 is not in permit range[0,3]");
                      this.assertThrows(
                          IllegalArgumentException.class,
                          () -> metadata.getColumnMetadata(5),
                          "Column index 5 is not in permit range[0,3]");
                      ColumnMetadata colMeta = metadata.getColumnMetadata(0);
                      assertEquals(String.class, colMeta.getJavaType());
                      assertEquals("t1", colMeta.getName());
                      assertEquals(Nullability.NON_NULL, colMeta.getNullability());
                      assertEquals(1024, colMeta.getPrecision());
                      assertEquals(0, colMeta.getScale());
                      assertEquals(
                          ColumnDefinitionPacket.class, colMeta.getNativeTypeMetadata().getClass());

                      colMeta = metadata.getColumnMetadata("t2");
                      assertEquals(Integer.class, colMeta.getJavaType());
                      assertEquals("t2", colMeta.getName());

                      this.assertThrows(
                          IllegalArgumentException.class,
                          () -> metadata.getColumnMetadata("wrongName"),
                          "Column name 'wrongName' does not exist in column names [t1, t2, t3, t4]");

                      colMeta = metadata.getColumnMetadata(1);
                      assertEquals(Integer.class, colMeta.getJavaType());
                      assertEquals("t2", colMeta.getName());
                      assertEquals(Nullability.NULLABLE, colMeta.getNullability());
                      assertEquals(11, colMeta.getPrecision());
                      assertEquals(0, colMeta.getScale());
                      assertEquals(
                          ColumnDefinitionPacket.class, colMeta.getNativeTypeMetadata().getClass());

                      colMeta = metadata.getColumnMetadata(2);
                      assertEquals(BigDecimal.class, colMeta.getJavaType());
                      assertEquals("t3", colMeta.getName());
                      assertEquals(Nullability.NULLABLE, colMeta.getNullability());
                      assertEquals(10, colMeta.getPrecision());
                      assertEquals(6, colMeta.getScale());
                      assertEquals(
                          ColumnDefinitionPacket.class, colMeta.getNativeTypeMetadata().getClass());

                      colMeta = metadata.getColumnMetadata(3);
                      assertEquals(BigDecimal.class, colMeta.getJavaType());
                      assertEquals("t4", colMeta.getName());
                      assertEquals(Nullability.NULLABLE, colMeta.getNullability());
                      assertEquals(10, colMeta.getPrecision());
                      assertEquals(6, colMeta.getScale());
                      assertEquals(
                          ColumnDefinitionPacket.class, colMeta.getNativeTypeMetadata().getClass());

                      Iterator<? extends ColumnMetadata> metas =
                          metadata.getColumnMetadatas().iterator();
                      assertEquals("t1", metas.next().getName());
                      assertEquals("t2", metas.next().getName());
                      assertEquals("t3", metas.next().getName());
                      assertEquals("t4", metas.next().getName());
                      assertFalse(metas.hasNext());
                      return Optional.ofNullable(row.get(0));
                    }))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.of("3"))
        .verifyComplete();
  }

  @Test
  void rowMetaString() {
    sharedConn
        .createStatement("SELECT * FROM rowMeta WHERE 1 = ?")
        .bind(0, 1)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      assertEquals(
                          "MariadbRowMetadata{columnNames=[t1, t2, t3, t4]}", metadata.toString());
                      return Optional.ofNullable(row.get(0));
                    }))
        .as(StepVerifier::create)
        .expectNext(Optional.of("some🌟"), Optional.of("1"), Optional.of("0"), Optional.of("3"))
        .verifyComplete();
  }
}
