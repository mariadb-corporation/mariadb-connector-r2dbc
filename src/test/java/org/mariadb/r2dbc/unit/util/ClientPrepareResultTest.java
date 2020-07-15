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

package org.mariadb.r2dbc.unit.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.util.ClientPrepareResult;

public class ClientPrepareResultTest {

  private void checkParsing(
      String sql,
      int paramNumber,
      boolean allowMultiqueries,
      boolean returning,
      boolean supportReturningAddition,
      String[] partsMulti) {
    ClientPrepareResult res = ClientPrepareResult.parameterParts(sql, false);
    Assertions.assertEquals(paramNumber, res.getParamCount());
    Assertions.assertEquals(returning, res.isReturning());
    Assertions.assertEquals(supportReturningAddition, res.supportAddingReturning());

    for (int i = 0; i < partsMulti.length; i++) {
      Assertions.assertEquals(partsMulti[i], new String(res.getQueryParts().get(i)));
    }
    Assertions.assertEquals(allowMultiqueries, res.isQueryMultipleRewritable());
  }

  @Test
  public void stringEscapeParsing() throws Exception {
    checkParsing(
        "select '\\'' as a, ? as b, \"\\\"\" as c, ? as d",
        2,
        true,
        false,
        false,
        new String[] {"select '\\'' as a, ", " as b, \"\\\"\" as c, ", " as d"});
  }

  @Test
  public void testRewritableWithConstantParameter() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=col2+10",
        2,
        true,
        false,
        true,
        new String[] {
          "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ",
          ", 5, ",
          ", 8) ON DUPLICATE KEY UPDATE col2=col2+10"
        });
  }

  @Test
  public void testComment() throws Exception {
    checkParsing(
        "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " INSERT into "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " tt VALUES "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " (?) "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */",
        1,
        true,
        false,
        true,
        new String[] {
          "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
              + " INSERT into "
              + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
              + " tt VALUES "
              + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
              + " (",
          ") " + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
        });
  }

  @Test
  public void testRewritableWithConstantParameterAndParamAfterValue() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=?",
        3,
        true,
        false,
        true,
        new String[] {
          "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ",
          ", 5, ",
          ", 8) ON DUPLICATE KEY UPDATE col2=",
          ""
        });
  }

  @Test
  public void testRewritableMultipleInserts() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2) VALUES (?, ?), (?, ?)",
        4,
        true,
        false,
        true,
        new String[] {"INSERT INTO TABLE(col1,col2) VALUES (", ", ", "), (", ", ", ")"});
  }

  @Test
  public void testCall() throws Exception {
    checkParsing(
        "CALL dsdssd(?,?)", 2, true, false, false, new String[] {"CALL dsdssd(", ",", ")"});
  }

  @Test
  public void testUpdate() throws Exception {
    checkParsing(
        "UPDATE MultiTestt4 SET test = ? WHERE test = ?",
        2,
        true,
        false,
        true,
        new String[] {"UPDATE MultiTestt4 SET test = ", " WHERE test = ", ""});
  }

  @Test
  public void testInsertSelect() throws Exception {
    checkParsing(
        "insert into test_insert_select ( field1) (select  TMP.field1 from "
            + "(select CAST(? as binary) `field1` from dual) TMP)",
        1,
        true,
        false,
        true,
        new String[] {
          "insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(",
          " as binary) `field1` from dual) TMP)"
        });
  }

  @Test
  public void testWithoutParameter() throws Exception {
    checkParsing(
        "SELECT testFunction()", 0, true, false, false, new String[] {"SELECT testFunction()"});
  }

  @Test
  public void testWithoutParameterAndParenthesis() throws Exception {
    checkParsing("SELECT 1", 0, true, false, false, new String[] {"SELECT 1"});
  }

  @Test
  public void testWithoutParameterAndValues() throws Exception {
    checkParsing(
        "INSERT INTO tt VALUES (1)",
        0,
        true,
        false,
        true,
        new String[] {"INSERT INTO tt VALUES (1)"});
  }

  @Test
  public void testSemiColon() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')",
        1,
        true,
        false,
        true,
        new String[] {
          "INSERT INTO tt (tt) VALUES (", "); INSERT INTO tt (tt) VALUES ('multiple')"
        });
  }

  @Test
  public void testSemicolonRewritableIfAtEnd() throws Exception {
    checkParsing(
        "INSERT INTO table (column1) VALUES (?); ",
        1,
        false,
        false,
        true,
        new String[] {"INSERT INTO table (column1) VALUES (", "); "});
  }

  @Test
  public void testSemicolonNotRewritableIfNotAtEnd() throws Exception {
    checkParsing(
        "INSERT INTO table (column1) VALUES (?); SELECT 1",
        1,
        true,
        false,
        true,
        new String[] {"INSERT INTO table (column1) VALUES (", "); SELECT 1"});
  }

  @Test
  public void testError() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')",
        1,
        true,
        false,
        true,
        new String[] {
          "INSERT INTO tt (tt) VALUES (", "); INSERT INTO tt (tt) VALUES ('multiple')"
        });
  }

  @Test
  public void testLineComment() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?) --fin",
        1,
        false,
        false,
        true,
        new String[] {"INSERT INTO tt (tt) VALUES (", ") --fin"});
  }

  @Test
  public void testEscapeInString() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?, '\\'?', \"\\\"?\") --fin",
        1,
        false,
        false,
        true,
        new String[] {"INSERT INTO tt (tt) VALUES (", ", '\\'?', \"\\\"?\") --fin"});
  }

  @Test
  public void testEol() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?, //test \n ?)",
        2,
        true,
        false,
        true,
        new String[] {"INSERT INTO tt (tt) VALUES (", ", //test \n ", ")"});
  }

  @Test
  public void testLineCommentFinished() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES --fin\n (?)",
        1,
        true,
        false,
        true,
        new String[] {"INSERT INTO tt (tt) VALUES --fin\n (", ")"});
  }

  @Test
  public void testSelect1() throws Exception {
    checkParsing("SELECT 1", 0, true, false, false, new String[] {"SELECT 1"});
  }

  @Test
  public void testLastInsertId() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?)",
        1,
        true,
        false,
        true,
        new String[] {"INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ")"});
  }

  @Test
  public void testReturning() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?) # test \n RETURNING ID",
        1,
        true,
        true,
        false,
        new String[] {
          "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ") # test \n RETURNING ID"
        });
    checkParsing(
        "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)",
        1,
        true,
        false,
        true,
        new String[] {"INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ", _RETURNING)"});
    checkParsing(
        "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)",
        1,
        true,
        false,
        true,
        new String[] {"INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ", _RETURNING)"});
    checkParsing(
        "DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?) RETURNING ID",
        1,
        true,
        true,
        false,
        new String[] {"DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ") RETURNING ID"});
    checkParsing(
        "DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)",
        1,
        true,
        false,
        true,
        new String[] {"DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ", _RETURNING)"});
    checkParsing(
        "DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)",
        1,
        true,
        false,
        true,
        new String[] {"DELETE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ", _RETURNING)"});
    checkParsing(
        "UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?) RETURNING ID",
        1,
        true,
        true,
        false,
        new String[] {"UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ") RETURNING ID"});
    checkParsing(
        "UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)",
        1,
        true,
        false,
        true,
        new String[] {"UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ", _RETURNING)"});
    checkParsing(
        "UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?, _RETURNING)",
        1,
        true,
        false,
        true,
        new String[] {"UPDATE tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ", _RETURNING)"});
  }

  @Test
  public void testValuesForPartition() throws Exception {
    checkParsing(
        "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
            + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01', '\"', \"'\"))",
        0,
        true,
        false,
        false,
        new String[] {
          "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
              + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01', '\"', \"'\"))"
        });
  }

  @Test
  public void hasParameter() {
    Assertions.assertTrue(ClientPrepareResult.hasParameter("SELECT ?", false));
    Assertions.assertTrue(ClientPrepareResult.hasParameter("SELECT :param", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT ':param''", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT '?'", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT \"?\"", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT \"\\?\"", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT `?`", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT /*? */", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT //?\n '?'", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT #? \n '?'", false));
    Assertions.assertFalse(ClientPrepareResult.hasParameter("SELECT --? \n '?'", false));
  }
}
