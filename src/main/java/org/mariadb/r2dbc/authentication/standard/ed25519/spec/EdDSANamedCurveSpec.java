// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab
package org.mariadb.r2dbc.authentication.standard.ed25519.spec;

import org.mariadb.r2dbc.authentication.standard.ed25519.math.Curve;
import org.mariadb.r2dbc.authentication.standard.ed25519.math.GroupElement;
import org.mariadb.r2dbc.authentication.standard.ed25519.math.ed25519.ScalarOps;

/**
 * EdDSA Curve specification that can also be referred to by name.
 *
 * @author str4d
 */
public class EdDSANamedCurveSpec extends EdDSAParameterSpec {

  private static final long serialVersionUID = -4771155800270949200L;
  private final String name;

  public EdDSANamedCurveSpec(
      String name, Curve curve, String hashAlgo, ScalarOps sc, GroupElement B) {
    super(curve, hashAlgo, sc, B);
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
