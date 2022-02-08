// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab
package org.mariadb.r2dbc.authentication.standard.ed25519.spec;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import org.mariadb.r2dbc.authentication.standard.ed25519.math.Curve;
import org.mariadb.r2dbc.authentication.standard.ed25519.math.GroupElement;
import org.mariadb.r2dbc.authentication.standard.ed25519.math.ed25519.ScalarOps;

/**
 * Parameter specification for an EdDSA algorithm.
 *
 * @author str4d
 */
public class EdDSAParameterSpec implements AlgorithmParameterSpec, Serializable {

  private static final long serialVersionUID = 8274987108472012L;
  private final Curve curve;
  private final String hashAlgo;
  private final ScalarOps sc;
  private final GroupElement B;

  /**
   * @param curve the curve
   * @param hashAlgo the JCA string for the hash algorithm
   * @param sc the parameter L represented as ScalarOps
   * @param B the parameter B
   * @throws IllegalArgumentException if hash algorithm is unsupported or length is wrong
   */
  public EdDSAParameterSpec(Curve curve, String hashAlgo, ScalarOps sc, GroupElement B) {
    try {
      MessageDigest hash = MessageDigest.getInstance(hashAlgo);
      // EdDSA hash function must produce 2b-bit output
      if (curve.getField().getb() / 4 != hash.getDigestLength()) {
        throw new IllegalArgumentException("Hash output is not 2b-bit");
      }
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Unsupported hash algorithm");
    }

    this.curve = curve;
    this.hashAlgo = hashAlgo;
    this.sc = sc;
    this.B = B;
  }

  public Curve getCurve() {
    return curve;
  }

  public String getHashAlgorithm() {
    return hashAlgo;
  }

  public ScalarOps getScalarOps() {
    return sc;
  }

  /** @return the base (generator) */
  public GroupElement getB() {
    return B;
  }

  @Override
  public int hashCode() {
    return hashAlgo.hashCode() ^ curve.hashCode() ^ B.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof EdDSAParameterSpec)) {
      return false;
    }
    EdDSAParameterSpec s = (EdDSAParameterSpec) o;
    return hashAlgo.equals(s.getHashAlgorithm())
        && curve.equals(s.getCurve())
        && B.equals(s.getB());
  }
}
