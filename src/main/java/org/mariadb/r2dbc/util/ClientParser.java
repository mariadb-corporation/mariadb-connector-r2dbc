// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ClientParser implements PrepareResult {

  private final String sql;
  private final byte[] query;
  private final List<Integer> paramPositions;
  private final List<String> paramNameList;
  private final int paramCount;
  private final Boolean isReturning;
  private final Boolean supportAddingReturning;

  private ClientParser(
      String sql,
      byte[] query,
      Boolean isReturning,
      Boolean supportAddingReturning,
      List<Integer> paramPositions,
      List<String> paramNameList) {
    this.sql = sql;
    this.query = query;

    this.paramPositions = paramPositions;
    this.paramNameList = paramNameList;
    if (paramNameList.isEmpty()) {
      this.paramCount = paramPositions.size() / 2;
    } else {
      this.paramCount = paramNameList.size();
    }
    this.isReturning = isReturning;
    this.supportAddingReturning = supportAddingReturning;
  }

  /**
   * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
   * list is separed by ? that are not in comments. isQueryMultipleRewritable flag is set if query
   * can be rewrite in one query (all case but if using "-- comment"). example for query : "INSERT
   * INTO tableName(id, name) VALUES (?, ?)" result list will be : {"INSERT INTO tableName(id, name)
   * VALUES (", ", ", ")"}
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientPrepareResult
   */
  public static ClientParser parameterPartsCheckReturning(
      String queryString, boolean noBackslashEscapes) {

    List<Integer> paramPositions = new ArrayList<>();
    List<String> paramNameList = new ArrayList<>();
    LexState state = LexState.Normal;
    byte lastChar = 0x00;
    boolean singleQuotes = false;
    boolean returning = false;
    boolean supportAddingReturning = false;
    byte[] query = queryString.getBytes(StandardCharsets.UTF_8);
    int queryLength = query.length;

    for (int i = 0; i < queryLength; i++) {

      byte car = query[i];
      if (state == LexState.Escape
          && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
        state = LexState.String;
        lastChar = car;
        continue;
      }
      switch (car) {
        case (byte) '*':
          if (state == LexState.Normal && lastChar == (byte) '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case (byte) '/':
          if (state == LexState.SlashStarComment && lastChar == (byte) '*') {
            state = LexState.Normal;
          } else if (state == LexState.Normal && lastChar == (byte) '/') {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '#':
          if (state == LexState.Normal) {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '-':
          if (state == LexState.Normal && lastChar == (byte) '-') {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '\n':
          if (state == LexState.EOLComment) {
            state = LexState.Normal;
          }
          break;

        case (byte) '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;

        case (byte) '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;
        case ':':
          if (state == LexState.Normal) {
            int beginPos = i;
            paramPositions.add(i);
            while (++i < queryLength
                && (car = query[i]) != ' '
                && ((car >= '0' && car <= '9')
                    || (car >= 'A' && car <= 'Z')
                    || (car >= 'a' && car <= 'z')
                    || car == '-'
                    || car == '_')) {}
            paramNameList.add(new String(query, beginPos + 1, i - (beginPos + 1)));
            paramPositions.add(i);
          }
          break;
        case (byte) '\\':
          if (noBackslashEscapes) {
            break;
          }
          if (state == LexState.String) {
            state = LexState.Escape;
          }
          break;
        case (byte) '?':
          if (state == LexState.Normal) {
            paramPositions.add(i);
            paramPositions.add(i + 1);
          }
          break;
        case (byte) '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;

        case 'r':
        case 'R':
          if (state == LexState.Normal
              && !returning
              && queryLength > i + 8
              && (query[i + 1] == 'e' || query[i + 1] == 'E')
              && (query[i + 2] == 't' || query[i + 2] == 'T')
              && (query[i + 3] == 'u' || query[i + 3] == 'U')
              && (query[i + 4] == 'r' || query[i + 4] == 'R')
              && (query[i + 5] == 'n' || query[i + 5] == 'N')
              && (query[i + 6] == 'i' || query[i + 6] == 'I')
              && (query[i + 7] == 'n' || query[i + 7] == 'N')
              && (query[i + 8] == 'g' || query[i + 8] == 'G')) {

            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              break;
            }
            if (i + 9 < queryLength
                && query[i + 9] > ' '
                && "();><=-+,".indexOf(query[i + 9]) == -1) {
              break;
            }
            returning = true;
            supportAddingReturning = false;
            i += 8;
          }
          break;

        case 'i':
        case 'I':
          if (state == LexState.Normal
              && !returning
              && queryLength > i + 6
              && (query[i + 1] == 'n' || query[i + 1] == 'N')
              && (query[i + 2] == 's' || query[i + 2] == 'S')
              && (query[i + 3] == 'e' || query[i + 3] == 'E')
              && (query[i + 4] == 'r' || query[i + 4] == 'R')
              && (query[i + 5] == 't' || query[i + 5] == 'T')) {

            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              i += 6;
              break;
            }

            if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
              i += 6;
              break;
            }

            supportAddingReturning = true;
            i += 6;
          }
          break;

        case 'u':
        case 'U':
          if (state == LexState.Normal
              && !returning
              && queryLength > i + 6
              && (query[i + 1] == 'p' || query[i + 1] == 'P')
              && (query[i + 2] == 'd' || query[i + 2] == 'D')
              && (query[i + 3] == 'a' || query[i + 3] == 'A')
              && (query[i + 4] == 't' || query[i + 4] == 'T')
              && (query[i + 5] == 'e' || query[i + 5] == 'E')) {

            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              i += 6;
              break;
            }

            if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
              i += 6;
              break;
            }

            supportAddingReturning = true;
            i += 6;
          }
          break;

        case 'd':
        case 'D':
          if (state == LexState.Normal
              && !returning
              && queryLength > i + 6
              && (query[i + 1] == 'e' || query[i + 1] == 'E')
              && (query[i + 2] == 'l' || query[i + 2] == 'L')
              && (query[i + 3] == 'e' || query[i + 3] == 'E')
              && (query[i + 4] == 't' || query[i + 4] == 'T')
              && (query[i + 5] == 'e' || query[i + 5] == 'E')) {

            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              i += 6;
              break;
            }

            if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
              i += 6;
              break;
            }

            supportAddingReturning = true;
            i += 6;
          }
          break;
      }
      lastChar = car;
    }

    return new ClientParser(
        queryString, query, returning, supportAddingReturning, paramPositions, paramNameList);
  }

  /**
   * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
   * list is separed by ? that are not in comments. isQueryMultipleRewritable flag is set if query
   * can be rewrite in one query (all case but if using "-- comment"). example for query : "INSERT
   * INTO tableName(id, name) VALUES (?, ?)" result list will be : {"INSERT INTO tableName(id, name)
   * VALUES (", ", ", ")"}
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientPrepareResult
   */
  public static ClientParser parameterParts(String queryString, boolean noBackslashEscapes) {

    List<Integer> paramPositions = new ArrayList<>();
    List<String> paramNameList = new ArrayList<>();
    LexState state = LexState.Normal;
    byte lastChar = 0x00;
    boolean singleQuotes = false;
    byte[] query = queryString.getBytes(StandardCharsets.UTF_8);
    int queryLength = query.length;

    for (int i = 0; i < queryLength; i++) {

      byte car = query[i];
      if (state == LexState.Escape
          && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
        state = LexState.String;
        lastChar = car;
        continue;
      }
      switch (car) {
        case (byte) '*':
          if (state == LexState.Normal && lastChar == (byte) '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case (byte) '/':
          if (state == LexState.SlashStarComment && lastChar == (byte) '*') {
            state = LexState.Normal;
          } else if (state == LexState.Normal && lastChar == (byte) '/') {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '#':
          if (state == LexState.Normal) {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '-':
          if (state == LexState.Normal && lastChar == (byte) '-') {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '\n':
          if (state == LexState.EOLComment) {
            state = LexState.Normal;
          }
          break;

        case (byte) '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;

        case (byte) '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;
        case ':':
          if (state == LexState.Normal) {
            int beginPos = i;
            paramPositions.add(i);
            while (++i < queryLength
                && (car = query[i]) != ' '
                && ((car >= '0' && car <= '9')
                    || (car >= 'A' && car <= 'Z')
                    || (car >= 'a' && car <= 'z')
                    || car == '-'
                    || car == '_')) {}
            paramNameList.add(new String(query, beginPos + 1, i - (beginPos + 1)));
            paramPositions.add(i);
          }
          break;
        case (byte) '\\':
          if (noBackslashEscapes) {
            break;
          }
          if (state == LexState.String) {
            state = LexState.Escape;
          }
          break;
        case (byte) '?':
          if (state == LexState.Normal) {
            paramPositions.add(i);
            paramPositions.add(i + 1);
          }
          break;
        case (byte) '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;
      }
      lastChar = car;
    }

    return new ClientParser(queryString, query, null, null, paramPositions, paramNameList);
  }

  /**
   * Check if SQL has parameter.
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return True if has named parameter
   */
  public static boolean hasParameter(String queryString, boolean noBackslashEscapes) {

    LexState state = LexState.Normal;
    char lastChar = '\0';
    boolean singleQuotes = false;

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
          if (noBackslashEscapes) {
            break;
          }
          if (state == LexState.String) {
            state = LexState.Escape;
          }
          break;

        case '?':
          if (state == LexState.Normal) {
            return true;
          }
          break;

        case ':':
          if (state == LexState.Normal) {
            while (++i < queryLength
                && (car = query[i]) != ' '
                && ((car >= '0' && car <= '9')
                    || (car >= 'A' && car <= 'Z')
                    || (car >= 'a' && car <= 'z')
                    || car == '-'
                    || car == '_')) {}
            return true;
          }
          break;

        case '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;
      }
      lastChar = car;
    }
    return false;
  }

  public String getSql() {
    return sql;
  }

  public byte[] getQuery() {
    return query;
  }

  public List<Integer> getParamPositions() {
    return paramPositions;
  }

  public List<String> getParamNameList() {
    return paramNameList;
  }

  public int getParamCount() {
    return paramCount;
  }

  public boolean isReturning() {
    return isReturning;
  }

  public Boolean supportAddingReturning() {
    return supportAddingReturning;
  }

  public void validateAddingReturning() {

    if (isReturning) {
      throw new IllegalStateException("Statement already includes RETURNING clause");
    }

    if (!supportAddingReturning) {
      throw new IllegalStateException("Cannot add RETURNING clause to query");
    }
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
