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
package org.mariadb.r2dbc.authentication.ed25519.spec;

import org.mariadb.r2dbc.authentication.ed25519.Utils;
import org.mariadb.r2dbc.authentication.ed25519.math.Curve;
import org.mariadb.r2dbc.authentication.ed25519.math.Field;
import org.mariadb.r2dbc.authentication.ed25519.math.ed25519.Ed25519LittleEndianEncoding;
import org.mariadb.r2dbc.authentication.ed25519.math.ed25519.ScalarOps;

import java.util.Hashtable;
import java.util.Locale;

/**
 * The named EdDSA curves.
 *
 * @author str4d
 */
public class EdDSANamedCurveTable {

  public static final String ED_25519 = "Ed25519";

  private static final Field ed25519field =
      new Field(
          256, // b
          Utils.hexToBytes("edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f"), // q
          new Ed25519LittleEndianEncoding());

  private static final Curve ed25519curve =
      new Curve(
          ed25519field,
          Utils.hexToBytes("a3785913ca4deb75abd841414d0a700098e879777940c78c73fe6f2bee6c0352"), // d
          ed25519field.fromByteArray(
              Utils.hexToBytes(
                  "b0a00e4a271beec478e42fad0618432fa7d7fb3d99004d2b0bdfc14f8024832b"))); // I

  private static final EdDSANamedCurveSpec ed25519 =
      new EdDSANamedCurveSpec(
          ED_25519,
          ed25519curve,
          "SHA-512", // H
          new ScalarOps(), // l
          ed25519curve.createPoint( // B
              Utils.hexToBytes("5866666666666666666666666666666666666666666666666666666666666666"),
              true)); // Precompute tables for B

  private static final Hashtable<String, EdDSANamedCurveSpec> curves =
      new Hashtable<String, EdDSANamedCurveSpec>();

  static {
    // RFC 8032
    defineCurve(ed25519);
  }

  public static void defineCurve(EdDSANamedCurveSpec curve) {
    curves.put(curve.getName().toLowerCase(Locale.ENGLISH), curve);
  }

  static void defineCurveAlias(String name, String alias) {
    EdDSANamedCurveSpec curve = curves.get(name.toLowerCase(Locale.ENGLISH));
    if (curve == null) {
      throw new IllegalStateException();
    }
    curves.put(alias.toLowerCase(Locale.ENGLISH), curve);
  }

  public static EdDSANamedCurveSpec getByName(String name) {
    return curves.get(name.toLowerCase(Locale.ENGLISH));
  }
}
