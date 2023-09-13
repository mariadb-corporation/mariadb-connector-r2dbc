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
        Map out = new HashMap();
        StringBuilder sb = new StringBuilder();
        Parse state = Parse.Normal;
        boolean iskey = true;
        boolean singleQuotes = true;
        String key = null;

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
                    } else if (!singleQuotes) {
                        state = Parse.Normal;
                    }
                    break;

                case '\'':
                    if (state == Parse.Normal) {
                        state = Parse.String;
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
                            out.put(key,sb);
                        } else {
                            key = sb.toString().trim();
                            if (!key.isEmpty()) {
                                out.put(key,null);
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
                        sb = new StringBuilder();
                    }
                    break;

                default:
                    // nothing
            }

            sb.append(car);
        }

        if (!iskey) {
            out.put(key,sb);
        } else {
            String tmpkey = sb.toString().trim();
            out.put(tmpkey,null);
        }
        return out;
    }

    private enum Parse {
        Normal,
        String, /* inside string */
        Escape /* found backslash */
    }
}
