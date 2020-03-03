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

/** Note: concrete subclasses must implement hashCode() and equals() */
public abstract class FieldElement implements Serializable {

  private static final long serialVersionUID = 1239527465875676L;

  protected final Field f;

  public FieldElement(Field f) {
    if (null == f) {
      throw new IllegalArgumentException("field cannot be null");
    }
    this.f = f;
  }

  /**
   * Encode a FieldElement in its $(b-1)$-bit encoding.
   *
   * @return the $(b-1)$-bit encoding of this FieldElement.
   */
  public byte[] toByteArray() {
    return f.getEncoding().encode(this);
  }

  public abstract boolean isNonZero();

  public boolean isNegative() {
    return f.getEncoding().isNegative(this);
  }

  public abstract FieldElement add(FieldElement val);

  public FieldElement addOne() {
    return add(f.ONE);
  }

  public abstract FieldElement subtract(FieldElement val);

  public FieldElement subtractOne() {
    return subtract(f.ONE);
  }

  public abstract FieldElement negate();

  public FieldElement divide(FieldElement val) {
    return multiply(val.invert());
  }

  public abstract FieldElement multiply(FieldElement val);

  public abstract FieldElement square();

  public abstract FieldElement squareAndDouble();

  public abstract FieldElement invert();

  public abstract FieldElement pow22523();

  public abstract FieldElement cmov(FieldElement val, final int b);

  // Note: concrete subclasses must implement hashCode() and equals()
}
