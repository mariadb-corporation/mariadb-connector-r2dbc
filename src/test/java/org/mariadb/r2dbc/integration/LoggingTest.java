// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

public class LoggingTest extends BaseConnectionTest {

  @Test
  void basicLogging() throws IOException {
    Assumptions.assumeFalse(isXpand());
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("org.mariadb.r2dbc");
    Level initialLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    // logger.detachAndStopAllAppenders();

    LoggerContext context = new LoggerContext();
    FileAppender<ILoggingEvent> fa = new FileAppender<ILoggingEvent>();
    fa.setName("FILE");
    fa.setImmediateFlush(true);
    PatternLayoutEncoder pa = new PatternLayoutEncoder();
    pa.setPattern("%r %5p %c [%t] - %m%n");
    pa.setContext(context);
    pa.start();
    fa.setEncoder(pa);

    fa.setFile(tempFile.getPath());
    fa.setAppend(true);
    fa.setContext(context);
    fa.start();

    logger.addAppender(fa);

    MariadbConnectionConfiguration conf = TestConfiguration.defaultBuilder.build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(1)
        .verifyComplete();
    MariadbConnectionMetadata meta = connection.getMetadata();
    connection.close().block();

    String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
    String selectIsolation =
        "         +-------------------------------------------------+\r\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
            + "+--------+-------------------------------------------------+----------------+\r\n"
            + "|00000000| 30 01 00 00 03 53 45 54 20 20 6e 61 6d 65 73 20 |0....SET  names |\r\n"
            + "|00000010| 55 54 46 38 4d 42 34 2c 61 75 74 6f 63 6f 6d 6d |UTF8MB4,autocomm|\r\n"
            + "|00000020| 69 74 3d 31 2c 74 78 5f 69 73 6f 6c 61 74 69 6f |it=1,tx_isolatio|\r\n"
            + "|00000030| 6e 3d 27 52 45 50 45 41 54 41 42 4c 45 2d 52 45 |n='REPEATABLE-RE|\r\n"
            + "|00000040| 41 44 27 2c 73 65 73 73 69 6f 6e 5f 74 72 61 63 |AD',session_trac|\r\n"
            + "|00000050| 6b 5f 73 63 68 65 6d 61 3d 31 2c 73 65 73 73 69 |k_schema=1,sessi|\r\n"
            + "|00000060| 6f 6e 5f 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f |on_track_system_|\r\n"
            + "|00000070| 76 61 72 69 61 62 6c 65 73 3d 49 46 28 40 40 73 |variables=IF(@@s|\r\n"
            + "|00000080| 65 73 73 69 6f 6e 5f 74 72 61 63 6b 5f 73 79 73 |ession_track_sys|\r\n"
            + "|00000090| 74 65 6d 5f 76 61 72 69 61 62 6c 65 73 20 3d 20 |tem_variables = |\r\n"
            + "|000000a0| 27 2a 27 2c 20 27 2a 27 2c 20 49 46 28 40 40 73 |'*', '*', IF(@@s|\r\n"
            + "|000000b0| 65 73 73 69 6f 6e 5f 74 72 61 63 6b 5f 73 79 73 |ession_track_sys|\r\n"
            + "|000000c0| 74 65 6d 5f 76 61 72 69 61 62 6c 65 73 20 3d 20 |tem_variables = |\r\n"
            + "|000000d0| 27 27 2c 20 27 61 75 74 6f 63 6f 6d 6d 69 74 2c |'', 'autocommit,|\r\n"
            + "|000000e0| 74 78 5f 69 73 6f 6c 61 74 69 6f 6e 27 2c 20 43 |tx_isolation', C|\r\n"
            + "|000000f0| 4f 4e 43 41 54 28 40 40 73 65 73 73 69 6f 6e 5f |ONCAT(@@session_|\r\n"
            + "|00000100| 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f 76 61 72 |track_system_var|\r\n"
            + "|00000110| 69 61 62 6c 65 73 2c 27 2c 61 75 74 6f 63 6f 6d |iables,',autocom|\r\n"
            + "|00000120| 6d 69 74 2c 74 78 5f 69 73 6f 6c 61 74 69 6f 6e |mit,tx_isolation|\r\n"
            + "|00000130| 27 29 29 29                                     |')))            |\r\n"
            + "+--------+-------------------------------------------------+----------------+";
    String transactionIsolation =
        "         +-------------------------------------------------+\r\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
            + "+--------+-------------------------------------------------+----------------+\r\n"
            + "|00000000| 4b 01 00 00 03 53 45 54 20 20 6e 61 6d 65 73 20 |K....SET  names |\r\n"
            + "|00000010| 55 54 46 38 4d 42 34 2c 61 75 74 6f 63 6f 6d 6d |UTF8MB4,autocomm|\r\n"
            + "|00000020| 69 74 3d 31 2c 74 72 61 6e 73 61 63 74 69 6f 6e |it=1,transaction|\r\n"
            + "|00000030| 5f 69 73 6f 6c 61 74 69 6f 6e 3d 27 52 45 50 45 |_isolation='REPE|\r\n"
            + "|00000040| 41 54 41 42 4c 45 2d 52 45 41 44 27 2c 73 65 73 |ATABLE-READ',ses|\r\n"
            + "|00000050| 73 69 6f 6e 5f 74 72 61 63 6b 5f 73 63 68 65 6d |sion_track_schem|\r\n"
            + "|00000060| 61 3d 31 2c 73 65 73 73 69 6f 6e 5f 74 72 61 63 |a=1,session_trac|\r\n"
            + "|00000070| 6b 5f 73 79 73 74 65 6d 5f 76 61 72 69 61 62 6c |k_system_variabl|\r\n"
            + "|00000080| 65 73 3d 49 46 28 40 40 73 65 73 73 69 6f 6e 5f |es=IF(@@session_|\r\n"
            + "|00000090| 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f 76 61 72 |track_system_var|\r\n"
            + "|000000a0| 69 61 62 6c 65 73 20 3d 20 27 2a 27 2c 20 27 2a |iables = '*', '*|\r\n"
            + "|000000b0| 27 2c 20 49 46 28 40 40 73 65 73 73 69 6f 6e 5f |', IF(@@session_|\r\n"
            + "|000000c0| 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f 76 61 72 |track_system_var|\r\n"
            + "|000000d0| 69 61 62 6c 65 73 20 3d 20 27 27 2c 20 27 61 75 |iables = '', 'au|\r\n"
            + "|000000e0| 74 6f 63 6f 6d 6d 69 74 2c 74 72 61 6e 73 61 63 |tocommit,transac|\r\n"
            + "|000000f0| 74 69 6f 6e 5f 69 73 6f 6c 61 74 69 6f 6e 27 2c |tion_isolation',|\r\n"
            + "|00000100| 20 43 4f 4e 43 41 54 28 40 40 73 65 73 73 69 6f | CONCAT(@@sessio|\r\n"
            + "|00000110| 6e 5f 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f 76 |n_track_system_v|\r\n"
            + "|00000120| 61 72 69 61 62 6c 65 73 2c 27 2c 61 75 74 6f 63 |ariables,',autoc|\r\n"
            + "|00000130| 6f 6d 6d 69 74 2c 74 72 61 6e 73 61 63 74 69 6f |ommit,transactio|\r\n"
            + "|00000140| 6e 5f 69 73 6f 6c 61 74 69 6f 6e 27 29 29 29    |n_isolation'))) |\r\n"
            + "+--------+-------------------------------------------------+----------------+";
    String mysqlIsolation =
        "         +-------------------------------------------------+\r\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
            + "+--------+-------------------------------------------------+----------------+\r\n"
            + "|00000000| 35 01 00 00 03 53 45 54 20 20 6e 61 6d 65 73 20 |5....SET  names |\r\n"
            + "|00000010| 55 54 46 38 4d 42 34 2c 61 75 74 6f 63 6f 6d 6d |UTF8MB4,autocomm|\r\n"
            + "|00000020| 69 74 3d 31 2c 74 72 61 6e 73 61 63 74 69 6f 6e |it=1,transaction|\r\n"
            + "|00000030| 5f 69 73 6f 6c 61 74 69 6f 6e 3d 27 52 45 50 45 |_isolation='REPE|\r\n"
            + "|00000040| 41 54 41 42 4c 45 2d 52 45 41 44 27 2c 73 65 73 |ATABLE-READ',ses|\r\n"
            + "|00000050| 73 69 6f 6e 5f 74 72 61 63 6b 5f 73 63 68 65 6d |sion_track_schem|\r\n"
            + "|00000060| 61 3d 31 2c 73 65 73 73 69 6f 6e 5f 74 72 61 63 |a=1,session_trac|\r\n"
            + "|00000070| 6b 5f 73 79 73 74 65 6d 5f 76 61 72 69 61 62 6c |k_system_variabl|\r\n"
            + "|00000080| 65 73 3d 49 46 28 40 40 73 65 73 73 69 6f 6e 5f |es=IF(@@session_|\r\n"
            + "|00000090| 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f 76 61 72 |track_system_var|\r\n"
            + "|000000a0| 69 61 62 6c 65 73 20 3d 20 27 2a 27 2c 20 27 2a |iables = '*', '*|\r\n"
            + "|000000b0| 27 2c 20 49 46 28 40 40 73 65 73 73 69 6f 6e 5f |', IF(@@session_|\r\n"
            + "|000000c0| 74 72 61 63 6b 5f 73 79 73 74 65 6d 5f 76 61 72 |track_system_var|\r\n"
            + "|000000d0| 69 61 62 6c 65 73 20 3d 20 27 27 2c 20 27 74 72 |iables = '', 'tr|\r\n"
            + "|000000e0| 61 6e 73 61 63 74 69 6f 6e 5f 69 73 6f 6c 61 74 |ansaction_isolat|\r\n"
            + "|000000f0| 69 6f 6e 27 2c 20 43 4f 4e 43 41 54 28 40 40 73 |ion', CONCAT(@@s|\r\n"
            + "|00000100| 65 73 73 69 6f 6e 5f 74 72 61 63 6b 5f 73 79 73 |ession_track_sys|\r\n"
            + "|00000110| 74 65 6d 5f 76 61 72 69 61 62 6c 65 73 2c 27 2c |tem_variables,',|\r\n"
            + "|00000120| 74 72 61 6e 73 61 63 74 69 6f 6e 5f 69 73 6f 6c |transaction_isol|\r\n"
            + "|00000130| 61 74 69 6f 6e 27 29 29 29                      |ation')))       |\r\n"
            + "+--------+-------------------------------------------------+----------------+";
    if ((meta.isMariaDBServer() && !meta.minVersion(11, 1, 1))
        || (meta.getMajorVersion() < 8 && !meta.minVersion(5, 7, 20))
        || (meta.getMajorVersion() >= 8 && !meta.minVersion(8, 0, 3))) {
      Assertions.assertTrue(
          contents.contains(selectIsolation)
              || contents.contains(selectIsolation.replace("\r\n", "\n")),
          contents);
    } else if (meta.isMariaDBServer()) {
      Assertions.assertTrue(
          contents.contains(transactionIsolation)
              || contents.contains(transactionIsolation.replace("\r\n", "\n")),
          contents);
    } else {
      Assertions.assertTrue(
          contents.contains(mysqlIsolation)
              || contents.contains(mysqlIsolation.replace("\r\n", "\n")),
          contents);
    }

    String selectOne =
        "         +-------------------------------------------------+\r\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
            + "+--------+-------------------------------------------------+----------------+\r\n"
            + "|00000000| 09 00 00 00 03 53 45 4c 45 43 54 20 31          |.....SELECT 1   |\r\n"
            + "+--------+-------------------------------------------------+----------------+";
    Assertions.assertTrue(
        contents.contains(selectOne) || contents.contains(selectOne.replace("\r\n", "\n")));
    logger.setLevel(initialLevel);
    logger.detachAppender(fa);
    logger.setAdditive(true);
  }

  public String encodeHexString(byte[] byteArray) {
    StringBuffer hexStringBuffer = new StringBuffer();
    for (int i = 0; i < byteArray.length; i++) {
      hexStringBuffer.append(byteToHex(byteArray[i]));
    }
    return hexStringBuffer.toString();
  }

  public String byteToHex(byte num) {
    char[] hexDigits = new char[2];
    hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
    hexDigits[1] = Character.forDigit((num & 0xF), 16);
    return new String(hexDigits);
  }
}
