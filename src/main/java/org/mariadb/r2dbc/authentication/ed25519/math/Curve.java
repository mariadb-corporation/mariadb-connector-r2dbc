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
package org.mariadb.r2dbc.authentication.ed25519.math;

import java.io.Serializable;

/**
 * A twisted Edwards curve. Points on the curve satisfy $-x^2 + y^2 = 1 + d x^2y^2$
 *
 * @author str4d
 */
public class Curve implements Serializable {

  private static final long serialVersionUID = 4578920872509827L;
  private final Field f;
  private final FieldElement d;
  private final FieldElement d2;
  private final FieldElement I;

  private final GroupElement zeroP2;
  private final GroupElement zeroP3;
  private final GroupElement zeroPrecomp;

  public Curve(Field f, byte[] d, FieldElement I) {
    this.f = f;
    this.d = f.fromByteArray(d);
    this.d2 = this.d.add(this.d);
    this.I = I;

    FieldElement zero = f.ZERO;
    FieldElement one = f.ONE;
    zeroP2 = GroupElement.p2(this, zero, one, one);
    zeroP3 = GroupElement.p3(this, zero, one, one, zero);
    zeroPrecomp = GroupElement.precomp(this, one, one, zero);
  }

  public Field getField() {
    return f;
  }

  public FieldElement getD() {
    return d;
  }

  public FieldElement get2D() {
    return d2;
  }

  public FieldElement getI() {
    return I;
  }

  public GroupElement getZero(GroupElement.Representation repr) {
    switch (repr) {
      case P2:
        return zeroP2;
      case P3:
        return zeroP3;
      case PRECOMP:
        return zeroPrecomp;
      default:
        return null;
    }
  }

  public GroupElement createPoint(byte[] P, boolean precompute) {
    GroupElement ge = new GroupElement(this, P);
    if (precompute) {
      ge.precompute(true);
    }
    return ge;
  }

  @Override
  public int hashCode() {
    return f.hashCode() ^ d.hashCode() ^ I.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Curve)) {
      return false;
    }
    Curve c = (Curve) o;
    return f.equals(c.getField()) && d.equals(c.getD()) && I.equals(c.getI());
  }
}
