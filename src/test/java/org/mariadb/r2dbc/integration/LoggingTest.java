// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

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
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("org.mariadb.r2dbc");
    Level initialLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    logger.detachAndStopAllAppenders();

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
        .then(
            () -> {
              MariadbConnectionMetadata meta = connection.getMetadata();
              connection.close().block();
              try {
                String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
                String selectIsolation =
                    "         +-------------------------------------------------+\r\n"
                        + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
                        + "+--------+-------------------------------------------------+----------------+\r\n"
                        + "|00000000| 16 00 00 00 03 53 45 4c 45 43 54 20 40 40 74 78 |.....SELECT @@tx|\r\n"
                        + "|00000010| 5f 69 73 6f 6c 61 74 69 6f 6e                   |_isolation      |\r\n"
                        + "+--------+-------------------------------------------------+----------------+";
                String mysqlIsolation =
                    "         +-------------------------------------------------+\r\n"
                        + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
                        + "+--------+-------------------------------------------------+----------------+\r\n"
                        + "|00000000| 1f 00 00 00 03 53 45 4c 45 43 54 20 40 40 74 72 |.....SELECT @@tr|\r\n"
                        + "|00000010| 61 6e 73 61 63 74 69 6f 6e 5f 69 73 6f 6c 61 74 |ansaction_isolat|\r\n"
                        + "|00000020| 69 6f 6e                                        |ion             |\r\n"
                        + "+--------+-------------------------------------------------+----------------+";

                if (meta.isMariaDBServer()
                    || (meta.getMajorVersion() < 8 && !meta.minVersion(5, 7, 20))
                    || (meta.getMajorVersion() >= 8 && !meta.minVersion(8, 0, 3))) {
                  Assertions.assertTrue(
                      contents.contains(selectIsolation)
                          || contents.contains(selectIsolation.replace("\r\n", "\n")));
                } else {
                  Assertions.assertTrue(
                      contents.contains(mysqlIsolation)
                          || contents.contains(mysqlIsolation.replace("\r\n", "\n")));
                }

                String selectOne =
                    "         +-------------------------------------------------+\r\n"
                        + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
                        + "+--------+-------------------------------------------------+----------------+\r\n"
                        + "|00000000| 09 00 00 00 03 53 45 4c 45 43 54 20 31          |.....SELECT 1   |\r\n"
                        + "+--------+-------------------------------------------------+----------------+";
                Assertions.assertTrue(
                    contents.contains(selectOne)
                        || contents.contains(selectOne.replace("\r\n", "\n")));
                String rowResult =
                    "         +-------------------------------------------------+\r\n"
                        + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
                        + "+--------+-------------------------------------------------+----------------+\r\n"
                        + "|00000000| 01 00 00 00 01                                  |.....           |\r\n"
                        + "+--------+-------------------------------------------------+----------------+";
                Assertions.assertTrue(
                    contents.contains(rowResult)
                        || contents.contains(rowResult.replace("\r\n", "\n")));
                logger.setLevel(initialLevel);
                logger.detachAppender(fa);
              } catch (IOException e) {
                e.printStackTrace();
                Assertions.fail();
              }
            })
        .verifyComplete();
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
