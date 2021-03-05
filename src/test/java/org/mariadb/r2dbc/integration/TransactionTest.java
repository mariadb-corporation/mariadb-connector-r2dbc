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

import io.r2dbc.spi.*;
import java.net.URL;
import java.util.*;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class TransactionTest extends BaseConnectionTest {
  private static String insertCmd =
      "INSERT INTO `users` (`first_name`, `last_name`, `email`) VALUES ('MariaDB', 'Row', 'mariadb@test.com')";

  @BeforeAll
  public static void before2() {
    drop();
    sharedConn
        .createStatement(
            "CREATE TABLE `users` (\n"
                + " `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                + " `first_name` varchar(255) COLLATE utf16_slovak_ci NOT NULL,\n"
                + " `last_name` varchar(255) COLLATE utf16_slovak_ci NOT NULL,\n"
                + " `email` varchar(255) COLLATE utf16_slovak_ci NOT NULL,\n"
                + " PRIMARY KEY (`id`)\n"
                + ")")
        .execute()
        .blockLast();
  }

  @BeforeEach
  public void beforeEch() {
    sharedConn.createStatement("TRUNCATE TABLE `users`").execute().blockLast();
  }

  @AfterAll
  public static void drop() {
    sharedConn.createStatement("DROP TABLE IF EXISTS `users`").execute().blockLast();
  }

  @Test
  void commit() {
    MariadbConnection conn = factory.create().block();

    conn.beginTransaction()
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.commitTransaction()).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 1);
    conn.close();
  }

  @Test
  void multipleBegin() {
    MariadbConnection conn = factory.create().block();
    // must issue only one begin command
    conn.beginTransaction().thenMany(conn.beginTransaction()).blockLast();
    conn.beginTransaction().block();
    conn.close();
  }

  @Test
  void commitWithoutTransaction() {
    // must issue no commit command
    sharedConn.commitTransaction().thenMany(sharedConn.commitTransaction()).blockLast();
    sharedConn.commitTransaction().block();
  }

  @Test
  void rollbackWithoutTransaction() {
    // must issue no commit command
    sharedConn.rollbackTransaction().thenMany(sharedConn.rollbackTransaction()).blockLast();
    sharedConn.rollbackTransaction().block();
  }

  @Test
  void createSavepoint() {
    // must issue multiple savepoints
    sharedConn.createSavepoint("t").thenMany(sharedConn.createSavepoint("t")).blockLast();
    sharedConn.createSavepoint("t").block();
  }

  @Test
  void rollback() {
    MariadbConnection conn = factory.create().block();

    conn.beginTransaction()
        .thenMany(conn.createStatement(insertCmd).execute())
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    conn.rollbackTransaction().block();
    checkInserted(conn, 0);
    conn.close();
  }

  @Test
  void rollbackPipelining() {
    MariadbConnection conn = factory.create().block();

    conn.beginTransaction()
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 0);
    conn.close();
  }

  @Test
  void releaseSavepoint() throws Exception {
    String url =
        "http://secure:pwd@hist:3306/testr2?sslMode=ENABLE&serverSslCert=-----BEGIN CERTIFICATE-----\n"
            + "MIIELzCCAxegAwIBAgIUNz1kWFjbLVDWVCBnEAsf95UPMS8wDQYJKoZIhvcNAQEL\n"
            + "BQAwQTEQMA4GA1UEChMHTWFyaWFEQjEPMA0GA1UECxMGU2t5U1FMMRwwGgYDVQQD\n"
            + "ExNyb290LXBraS5za3lzcWwubmV0MB4XDTE5MDkwNTE4MjMxMVoXDTI5MDkwMjE4\n"
            + "MjM0MFowQTEQMA4GA1UEChMHTWFyaWFEQjEPMA0GA1UECxMGU2t5U1FMMRwwGgYD\n"
            + "VQQDExNyb290LXBraS5za3lzcWwubmV0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A\n"
            + "MIIBCgKCAQEA0orsl93gB0MZuiby/4QmcmK2OrS/hEcDI/AN+Dpn9c3JKeEVp2OD\n"
            + "sft47OHGwdgyagBtNV6zZgOc6IOnwt+rGDmrmiuxHkf/XWV+y66skWAtMyM7ycCL\n"
            + "J3z5dO6xaZvYKJyhPcnx2NROEAJrkdVfoyJCtCElDDdRrknXWLPfZrph8E7I2mDP\n"
            + "SV8ZF4wdxbU7oHKM4CoTRgXQnCDq2Wv8OLZr4Mq224nSmEJK+cXRwKqbFUvuiSco\n"
            + "bTBnJjyeKldqJ/lCRwu9fU6fBHFuBNUEvZBzavt0B8SYi/l22wYHxlpOslowTaG4\n"
            + "Lh8Nj79PP7rsy44hHvOBGc/ZsKIGCDOIMwIDAQABo4IBHTCCARkwDgYDVR0PAQH/\n"
            + "BAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFHAIHx8QSWENuYb/xmuh\n"
            + "17S2nnXWMB8GA1UdIwQYMBaAFHAIHx8QSWENuYb/xmuh17S2nnXWMEAGCCsGAQUF\n"
            + "BwEBBDQwMjAwBggrBgEFBQcwAoYkaHR0cDovLzEyNy4wLjAuMTo4MjAwL3YxL3Br\n"
            + "aV9yb290L2NhMB4GA1UdEQQXMBWCE3Jvb3QtcGtpLnNreXNxbC5uZXQwHAYDVR0e\n"
            + "AQH/BBIwEKAOMAyCCnNreXNxbC5uZXQwNgYDVR0fBC8wLTAroCmgJ4YlaHR0cDov\n"
            + "LzEyNy4wLjAuMTo4MjAwL3YxL3BraV9yb290L2NybDANBgkqhkiG9w0BAQsFAAOC\n"
            + "AQEAfwo8ZW666UjHJ4DY+M9tDgRwFwFd7v3EBhLrGvkD+CWaiJIS9RnWwE0gn9SU\n"
            + "syBvRn3PrjsveCR3cIjqAzUplOyMMvvJ77E8rzfQEwOhHbATyKNQG32KaitCdEBP\n"
            + "v0XDb7SBw2eKQxdahMcT5yxh9DkCizTXE8usZIiW+V9FVcEPPNia4d9ZMlmLWMcP\n"
            + "pZlxE4W5ngU6iCN7PJ3aeKrk4Y1PM36XJ11f5pouMULUvqbjepa/R1KJt27OSbrJ\n"
            + "RjHDa+s0AljgPZDl7KqQOOA5hrNT1Om+5IVs+uAbY7mWQC2GwYlFsg5laqWf7SC0\n"
            + "hPvVLDb8GaRK6LA4PCROZwiM9g==\n"
            + "-----END CERTIFICATE-----\n"
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIID4jCCAsqgAwIBAgIUB9abD+hn9+dtYuqwkA3Rj5ZrPgUwDQYJKoZIhvcNAQEL\n"
            + "BQAwQTEQMA4GA1UEChMHTWFyaWFEQjEPMA0GA1UECxMGU2t5U1FMMRwwGgYDVQQD\n"
            + "ExNyb290LXBraS5za3lzcWwubmV0MB4XDTE5MDkwNTE4MjMxOFoXDTI5MDkwMjE4\n"
            + "MjM0OFowGTEXMBUGA1UEAxMOcGtpLnNreXNxbC5uZXQwggEiMA0GCSqGSIb3DQEB\n"
            + "AQUAA4IBDwAwggEKAoIBAQDYov+F2ijXdIiZ0AuX4fAJ6KQ16zb4mQ2qgsrO02yW\n"
            + "kF3EJV6/XQO0WqGok4SjcvLBLuSsQBFahtgB70d/YZ+PBUrwzzmgWa3Ga+GuzKl6\n"
            + "O2QI8vu7l8D0esJe7mY4KsAwNIvMUAdqUUCgB01KmCIwWoVqN1h65dX1qOf1N6qk\n"
            + "f+rXeFKBGoDq/DM6zR90irpYBt2guE5iZd5r63JMXANlmh44IWxEswBqAa4B+GlY\n"
            + "7m7Psk9E+i4rexN45+815SLnHr86y3PNUlFfzgfQwXCLVPqSPGfNzyEz8MZUVYQv\n"
            + "zg+hZmrTe9nCekMi3hk97Dh1x40Y1rSTtbQjUu2SvJepAgMBAAGjgfkwgfYwDgYD\n"
            + "VR0PAQH/BAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFLwrAxEtsiHF\n"
            + "Oz/dRW65RxwIHHVmMB8GA1UdIwQYMBaAFHAIHx8QSWENuYb/xmuh17S2nnXWMEAG\n"
            + "CCsGAQUFBwEBBDQwMjAwBggrBgEFBQcwAoYkaHR0cDovLzEyNy4wLjAuMTo4MjAw\n"
            + "L3YxL3BraV9yb290L2NhMBkGA1UdEQQSMBCCDnBraS5za3lzcWwubmV0MDYGA1Ud\n"
            + "HwQvMC0wK6ApoCeGJWh0dHA6Ly8xMjcuMC4wLjE6ODIwMC92MS9wa2lfcm9vdC9j\n"
            + "cmwwDQYJKoZIhvcNAQELBQADggEBAAZZKyFT+mVwuafkBOBYqXb/dCPdqbUnGBic\n"
            + "E2dRK0sxKYbeq7I3lo95UXrtfNBEMY740ZJzUwi6whDUMGNMoV0yFRPHPYvmopC5\n"
            + "wCUA62pPuvHEqwo7HSuO3TBmt5x0b2e9R0gJ535GZSTQI+ArseUwn5IJ2v/BUIRJ\n"
            + "xjAMwRmM9TOWcK6VLEBZoHXEzENrBLHr0fKhVyJhhuQV+xeEVY28odwzwH85AyUk\n"
            + "1lrAIzuz3YCDHtjL449U+hdz/2tytI1KXJscm/mrAhtgUjQnKCY0fFkJESL+TDwX\n"
            + "Sdb13rs8ZQvwanpcOt+Kg86O/vz2P5JLC8fK1L4aUilt0X5b8gc=\n"
            + "-----END CERTIFICATE-----";
    URL r = new URL(url);
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(Flux.from(conn.releaseSavepoint("mySavePoint")).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 2);
    conn.rollbackTransaction().block();
    conn.close();
  }

  @Test
  void rollbackSavepoint() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    conn.rollbackTransactionToSavepoint("mySavePoint").block();
    checkInserted(conn, 1);
    conn.rollbackTransaction().block();
    conn.close();
  }

  @Test
  void rollbackSavepointPipelining() {
    MariadbConnection conn = factory.create().block();
    conn.setAutoCommit(false).block();
    conn.createStatement(insertCmd)
        .execute()
        .thenMany(conn.createSavepoint("mySavePoint"))
        .thenMany(conn.createStatement(insertCmd).execute())
        .concatWith(
            Flux.from(conn.rollbackTransactionToSavepoint("mySavePoint")).then(Mono.empty()))
        .onErrorResume(err -> Flux.from(conn.rollbackTransaction()).then(Mono.empty()))
        .blockLast();
    checkInserted(conn, 1);
    conn.rollbackTransaction().block();
    conn.close();
  }

  private Mono<Void> checkInserted(MariadbConnection conn, int expectedValue) {
    conn.createStatement("SELECT count(*) FROM `users`")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(expectedValue)
        .verifyComplete();
    return Mono.empty();
  }
}
