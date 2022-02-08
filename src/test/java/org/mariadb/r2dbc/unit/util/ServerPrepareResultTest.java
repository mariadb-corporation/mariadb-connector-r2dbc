// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.ServerNamedParamParser;
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

  @Test
  public void stringReturningParsing() throws Exception {
    checkParsing("select * from t \t RETURNINGa()", 0, 0);
  }

  @Test
  public void stringEscapeParsing() throws Exception {
    checkParsing(
        "select '\\'\"`/*#' as a, ? as \\b, \"\\\"'returningInsertDeleteUpdate\" as c, ? as d",
        2,
        1);
  }

  @Test
  public void testRewritableWithConstantParameter() throws Exception {
    checkParsing(
        "INSERT INTO TABLE_INSERT(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=col2+10",
        2,
        2);
  }

  @Test
  public void testNamedParam() throws Exception {
    checkParsing(
        "SELECT * FROM TABLE WHERE 1 = :firstParam AND 3 = ':para' and 2 = :secondParam", 2, 2);
  }

  @Test
  public void stringEscapeParsing2() throws Exception {
    checkParsing("SELECT '\\\\test' /*test* #/ ;`*/", 0, 0);
  }

  @Test
  public void stringEscapeParsing3() throws Exception {
    checkParsing("DO '\\\"', \"\\'\"", 0, 0);
  }

  @Test
  public void testComment() throws Exception {
    checkParsing(
        "/* insert Select INSERT INTO tt VALUES (?,?,?,?) insert update delete select returning */"
            + " INSERT into "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " tt VALUES "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " (?) "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */",
        1,
        1);
  }

  @Test
  public void testRewritableWithConstantParameterAndParamAfterValue() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=?",
        3,
        3);
  }

  @Test
  public void testRewritableMultipleInserts() throws Exception {
    checkParsing("INSERT INTO TABLE(col1,col2) VALUES (?, ?), (?, ?)", 4, 4);
  }

  @Test
  public void testCall() throws Exception {
    checkParsing("CALL dsdssd(?,?)", 2, 2);
  }

  @Test
  public void testUpdate() throws Exception {
    checkParsing("UPDATE MultiTestt4 SET test = ? WHERE test = ?", 2, 2);
  }

  @Test
  public void testUpdate2() throws Exception {
    checkParsing("UPDATE UpdateMultiTestt4UPDATE() SET test = ? WHERE test = ?", 2, 2);
  }

  @Test
  public void testDelete() throws Exception {
    checkParsing("DELETE FROM MultiTestt4  WHERE test = ?", 1, 1);
  }

  @Test
  public void testDelete2() throws Exception {
    checkParsing("DELETE FROM DELETEMultiTestt4DELETE WHERE test = ?", 1, 1);
  }

  @Test
  public void testInsertSelect() throws Exception {
    checkParsing(
        "insert into test_insert_select ( field1) (select  TMP.field1 from "
            + "(select CAST(? as binary) `field1` from dual) TMP)",
        1,
        1);
  }

  @Test
  public void testWithoutParameter() throws Exception {
    checkParsing("SELECT testFunction()", 0, 0);
  }

  @Test
  public void testWithoutParameterAndParenthesis() throws Exception {
    checkParsing("SELECT 1", 0, 0);
  }

  @Test
  public void testWithoutParameterAndValues() throws Exception {
    checkParsing("INSERT INTO tt VALUES (1)", 0, 0);
  }

  @Test
  public void testSemiColon() throws Exception {
    checkParsing("INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')", 1, 1);
  }

  @Test
  public void testSemicolonRewritableIfAtEnd() throws Exception {
    checkParsing("INSERT INTO table (column1) VALUES (?); ", 1, 1);
  }

  @Test
  public void testSemicolonNotRewritableIfNotAtEnd() throws Exception {
    checkParsing("INSERT INTO table (column1) VALUES (?); SELECT 1", 1, 1);
  }

  @Test
  public void testError() throws Exception {
    checkParsing("INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')", 1, 1);
  }

  @Test
  public void testLineComment() throws Exception {
    checkParsing("INSERT INTO tt (tt) VALUES (?) --fin", 1, 1);
  }

  @Test
  public void testEscapeInString() throws Exception {
    checkParsing("INSERT INTO tt (tt) VALUES (?, '\\'?', \"\\\"?\") --fin", 1, 2);
  }

  @Test
  public void testEol() throws Exception {
    checkParsing("INSERT INTO tt (tt) VALUES (?, //test \n ?)", 2, 2);
  }

  @Test
  public void testLineCommentFinished() throws Exception {
    checkParsing("INSERT INTO tt (tt) VALUES --fin\n (?)", 1, 1);
  }

  @Test
  public void testSelect1() throws Exception {
    checkParsing("SELECT 1", 0, 0);
  }

  @Test
  public void testLastInsertId() throws Exception {
    checkParsing("INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?)", 1, 1);
  }

  @Test
  public void testReturning() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?) # test \n RETURNING ID", 1, 1);
    checkParsing("INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)", 1, 1);
    checkParsing("INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)", 1, 1);
    checkParsing("DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?) RETURNING ID", 1, 1);
    checkParsing("DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)", 1, 1);
    checkParsing("DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)", 1, 1);
    checkParsing("UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?) RETURNING ID", 1, 1);
    checkParsing("UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)", 1, 1);
    checkParsing("UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)", 1, 1);
  }

  @Test
  public void testValuesForPartition() throws Exception {
    checkParsing(
        "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
            + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01', '\"', \"'\"))",
        0,
        0);
  }

  @Test
  public void testEolskip() throws Exception {
    checkParsing("CREATE TABLE tt \n # test \n(ID INT)", 0, 0);
  }

  private void checkParsing(String sql, int paramNumber, int paramNumberBackSlash) {
    ServerNamedParamParser res = ServerNamedParamParser.parameterParts(sql, false);
    Assertions.assertEquals(paramNumber, res.getParamCount());
    res = ServerNamedParamParser.parameterParts(sql, true);
    Assertions.assertEquals(paramNumberBackSlash, res.getParamCount());
  }
}
