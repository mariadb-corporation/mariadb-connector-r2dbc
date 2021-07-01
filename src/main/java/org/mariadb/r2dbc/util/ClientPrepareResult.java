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

package org.mariadb.r2dbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ClientPrepareResult implements PrepareResult {

  private final List<byte[]> queryParts;
  private final List<String> paramNameList;
  private final int paramCount;
  private boolean isQueryMultipleRewritable;
  private boolean isReturning;
  private boolean supportAddingReturning;

  private ClientPrepareResult(
      List<byte[]> queryParts,
      List<String> paramNameList,
      boolean isQueryMultipleRewritable,
      boolean isReturning,
      boolean supportAddingReturning) {
    this.queryParts = queryParts;
    this.paramNameList = paramNameList;
    this.isQueryMultipleRewritable = isQueryMultipleRewritable;
    this.paramCount = queryParts.size() - 1;
    this.isReturning = isReturning;
    this.supportAddingReturning = supportAddingReturning;
  }

  /**
   * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
   * list is separated by ? or :name that are not in comments.
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientPrepareResult
   */
  public static ClientPrepareResult parameterParts(String queryString, boolean noBackslashEscapes) {
    boolean multipleQueriesPrepare = true;
    List<byte[]> partList = new ArrayList<>();
    List<String> paramNameList = new ArrayList<>();
    LexState state = LexState.Normal;
    char lastChar = '\0';
    boolean endingSemicolon = false;
    boolean returning = false;
    boolean supportAddingReturning = false;

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
            multipleQueriesPrepare = false;
          }
          break;

        case '\n':
          if (state == LexState.EOLComment) {
            multipleQueriesPrepare = true;
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
            multipleQueriesPrepare = false;
          }
          break;
        case '?':
          if (state == LexState.Normal) {
            partList.add(
                queryString.substring(lastParameterPosition, i).getBytes(StandardCharsets.UTF_8));
            lastParameterPosition = i + 1;
            paramNameList.add(null);
          }
          break;

        case ':':
          if (state == LexState.Normal) {
            partList.add(
                queryString.substring(lastParameterPosition, i).getBytes(StandardCharsets.UTF_8));
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
            multipleQueriesPrepare = true;
          }
          break;
      }
      lastChar = car;
    }
    if (lastParameterPosition == 0) {
      partList.add(queryString.getBytes(StandardCharsets.UTF_8));
    } else {
      partList.add(
          queryString
              .substring(lastParameterPosition, queryLength)
              .getBytes(StandardCharsets.UTF_8));
    }

    return new ClientPrepareResult(
        partList, paramNameList, multipleQueriesPrepare, returning, supportAddingReturning);
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

  public List<byte[]> getQueryParts() {
    return queryParts;
  }

  public List<String> getParamNameList() {
    return paramNameList;
  }

  public boolean isQueryMultipleRewritable() {
    return isQueryMultipleRewritable;
  }

  public boolean isReturning() {
    return isReturning;
  }

  public boolean supportAddingReturning() {
    return supportAddingReturning;
  }

  public int getParamCount() {
    return paramCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ClientPrepareResult{queryParts=[");
    for (int i = 0; i < queryParts.size(); i++) {
      byte[] part = queryParts.get(i);
      if (i != 0) sb.append(",");
      sb.append("'").append(new String(part, StandardCharsets.UTF_8)).append("'");
    }
    sb.append("], paramNameList=")
        .append(String.join(",", paramNameList))
        .append(", paramCount=")
        .append(paramCount)
        .append('}');
    return sb.toString();
  }

  public void validateAddingReturning() {

    if (isReturning()) {
      throw new IllegalStateException("Statement already includes RETURNING clause");
    }

    if (!supportAddingReturning()) {
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
