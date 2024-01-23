// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import java.util.ArrayList;
import java.util.List;

public class ServerNamedParamParser implements PrepareResult {

  private final String realSql;
  private final List<String> paramNameList;
  private final int paramCount;

  private ServerNamedParamParser(String realSql, List<String> paramNameList) {
    this.realSql = realSql;
    this.paramNameList = paramNameList;
    this.paramCount = paramNameList.size();
  }

  /**
   * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
   * list is separated by ? or :name that are not in comments.
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientPrepareResult
   */
  public static ServerNamedParamParser parameterParts(
      String queryString, boolean noBackslashEscapes) {
    StringBuilder sb = new StringBuilder();
    List<String> paramNameList = new ArrayList<>();

    LexState state = LexState.Normal;
    char lastChar = '\0';
    boolean endingSemicolon = false;

    boolean singleQuotes = false;
    int lastParameterPosition = 0;

    char[] query = queryString.toCharArray();
    int queryLength = query.length;
    for (int i = 0; i < queryLength; i++) {

      char car = query[i];
      if (state == LexState.Escape) {
        state = LexState.String;
        lastChar = car;
        continue;
      }

      switch (car) {
        case '*':
          if (state == LexState.Normal && lastChar == '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case '/':
          if (state == LexState.SlashStarComment && lastChar == '*') {
            state = LexState.Normal;
          } else if (state == LexState.Normal && lastChar == '/') {
            state = LexState.EOLComment;
          }
          break;

        case '#':
          if (state == LexState.Normal) {
            state = LexState.EOLComment;
          }
          break;

        case '-':
          if (state == LexState.Normal && lastChar == '-') {
            state = LexState.EOLComment;
          }
          break;

        case '\n':
          if (state == LexState.EOLComment) {
            state = LexState.Normal;
          }
          break;

        case '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          }
          break;

        case '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          }
          break;

        case '\\':
          if (!noBackslashEscapes && state == LexState.String) {
            state = LexState.Escape;
          }
          break;
        case ';':
          if (state == LexState.Normal) {
            endingSemicolon = true;
          }
          break;

        case '?':
          if (state == LexState.Normal) {
            sb.append(queryString, lastParameterPosition, i).append("?");
            lastParameterPosition = i + 1;
            paramNameList.add(null);
          }
          break;

        case ':':
          if (state == LexState.Normal) {
            sb.append(queryString, lastParameterPosition, i).append("?");
            String placeholderName = "";
            while (++i < queryLength
                && (car = query[i]) != ' '
                && ((car >= '0' && car <= '9')
                    || (car >= 'A' && car <= 'Z')
                    || (car >= 'a' && car <= 'z')
                    || car == '-'
                    || car == '_')) {
              placeholderName += car;
            }
            lastParameterPosition = i;
            paramNameList.add(placeholderName);
          }
          break;

        case '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;

        default:
          // multiple queries
          if (state == LexState.Normal && endingSemicolon && ((byte) car >= 40)) {
            endingSemicolon = false;
          }
          break;
      }
      lastChar = car;
    }
    if (lastParameterPosition == 0) {
      sb.append(queryString);
    } else {
      sb.append(queryString, lastParameterPosition, queryLength);
    }

    return new ServerNamedParamParser(sb.toString(), paramNameList);
  }

  public String getRealSql() {
    return realSql;
  }

  public List<String> getParamNameList() {
    return paramNameList;
  }

  @Override
  public int getParamCount() {
    return paramCount;
  }

  enum LexState {
    Normal, /* inside  query */
    String, /* inside string */
    SlashStarComment, /* inside slash-star comment */
    Escape, /* found backslash */
    EOLComment, /* # comment, or // comment, or -- comment */
    Backtick /* found backtick */
  }
}
