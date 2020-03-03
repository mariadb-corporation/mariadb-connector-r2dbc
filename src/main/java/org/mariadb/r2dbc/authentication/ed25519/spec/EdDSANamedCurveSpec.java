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

import org.mariadb.r2dbc.authentication.ed25519.math.Curve;
import org.mariadb.r2dbc.authentication.ed25519.math.GroupElement;
import org.mariadb.r2dbc.authentication.ed25519.math.ed25519.ScalarOps;

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
