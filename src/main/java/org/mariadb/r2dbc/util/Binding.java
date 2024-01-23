// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import java.util.Arrays;
import java.util.Objects;
import org.mariadb.r2dbc.MariadbCommonStatement;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class Binding {
  private static final Logger LOGGER = Loggers.getLogger(Binding.class);
  private final int expectedSize;
  private BindValue[] binds;
  private int currentSize = 0;

  public Binding(int expectedSize) {
    this.expectedSize = expectedSize;
    this.binds =
        new BindValue[(expectedSize == MariadbCommonStatement.UNKNOWN_SIZE) ? 10 : expectedSize];
  }

  public Binding add(int index, BindValue parameter) {
    if (index >= this.expectedSize) {
      if (expectedSize != MariadbCommonStatement.UNKNOWN_SIZE) {
        throw new IndexOutOfBoundsException(
            String.format(
                "Binding index %d when only %d parameters are expected", index, this.expectedSize));
      }
      grow(index + 1);
    }
    if (index >= currentSize) currentSize = index + 1;
    this.binds[index] = parameter;
    return this;
  }

  private void grow(int minLength) {
    int currLength = this.binds.length;
    int newLength = Math.max(currLength + (currLength >> 1), minLength);
    this.binds = Arrays.copyOf(this.binds, newLength);
  }

  public void clear() {
    this.binds = new BindValue[expectedSize];
    this.currentSize = 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Binding that = (Binding) o;
    return Objects.equals(this.binds, that.binds);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(expectedSize);
    result = 31 * result + Arrays.hashCode(binds);
    return result;
  }

  @Override
  public String toString() {
    return Arrays.toString(this.binds);
  }

  public void validate(int expectedSize) {
    // valid parameters
    for (int i = 0; i < expectedSize; i++) {
      if (binds[i] == null) {
        throw new IllegalStateException(String.format("Parameter at position %d is not set", i));
      }
    }
  }

  public BindValue[] getBindResultParameters(int paramNumber) {
    if (paramNumber == 0) {
      return new BindValue[0];
    }

    if (paramNumber < this.binds.length) {
      throw new IllegalStateException(
          String.format("No parameter specified for index %d", this.binds.length));
    }

    for (int i = 0; i < paramNumber; i++) {
      if (this.binds[i] == null) {
        throw new IllegalStateException(String.format("No parameter specified for index %d", i));
      }
    }
    if (paramNumber == expectedSize) return binds;
    if (paramNumber < expectedSize) {
      return Arrays.copyOfRange(binds, 0, paramNumber);
    }
    throw new IllegalStateException(
        String.format("No parameter specified for index %d", expectedSize));
  }

  public BindValue[] getBinds() {

    for (int i = 0; i < currentSize; i++) {
      if (this.binds[i] == null) {
        throw new IllegalStateException(String.format("No parameter specified for index %d", i));
      }
    }
    return Arrays.copyOfRange(binds, 0, currentSize);
  }
}
