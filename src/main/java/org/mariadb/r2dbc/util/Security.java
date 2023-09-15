package org.mariadb.r2dbc.util;

import java.util.HashMap;
import java.util.Map;

public class Security {

  /**
   * Parse the option "sessionVariable" to ensure having no injection. semi-column not in string
   * will be replaced by comma.
   *
   * @param sessionVariable option value
   * @return parsed String
   */
  public static Map parseSessionVariables(String sessionVariable) {
    Map<String, Object> out = new HashMap<>();
    StringBuilder sb = new StringBuilder();
    Parse state = Parse.Normal;
    boolean iskey = true;
    boolean singleQuotes = true;
    String key = null;
    boolean isString = false;

    char[] chars = sessionVariable.toCharArray();

    for (char car : chars) {

      if (state == Parse.Escape) {
        sb.append(car);
        state = Parse.String;
        continue;
      }

      switch (car) {
        case '"':
          if (state == Parse.Normal) {
            state = Parse.String;
            singleQuotes = false;
            isString = true;
          } else if (!singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\'':
          if (state == Parse.Normal) {
            state = Parse.String;
            isString = true;
            singleQuotes = true;
          } else if (singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\\':
          if (state == Parse.String) {
            state = Parse.Escape;
          }
          break;

        case ';':
        case ',':
          if (state == Parse.Normal) {
            if (!iskey) {
              String valStr = sb.toString().substring(1);
              out.put(key, parseObject(valStr, isString));
              isString = false;
            } else {
              key = sb.toString().trim();
              if (!key.isEmpty()) {
                out.put(key, null);
              }
            }
            iskey = true;
            key = null;
            sb = new StringBuilder();
            continue;
          }
          break;

        case '=':
          if (state == Parse.Normal && iskey) {
            key = sb.toString().trim();
            iskey = false;
            isString = false;
            sb = new StringBuilder();
          }
          break;

        default:
          // nothing
      }

      sb.append(car);
    }

    if (!iskey) {
      String valStr = sb.toString().substring(1);
      out.put(key, parseObject(valStr, isString));
    } else {
      String tmpkey = sb.toString().trim();
      out.put(tmpkey, null);
    }
    return out;
  }

  private static Object parseObject(String valStr, boolean isString) {
    if (!isString) {
      if (valStr.indexOf(".") > 0) {
        try {
          return Double.parseDouble(valStr);
        } catch (Exception e) {
        }
      } else {
        try {
          return Integer.parseInt(valStr);
        } catch (Exception e) {
        }
      }
    }
    return valStr;
  }

  private enum Parse {
    Normal,
    String, /* inside string */
    Escape /* found backslash */
  }
}
