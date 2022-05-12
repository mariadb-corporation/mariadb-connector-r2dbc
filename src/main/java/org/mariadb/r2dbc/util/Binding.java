// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.util.ReferenceCountUtil;
import java.util.*;
import reactor.core.publisher.Flux;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class Binding {
  private static final Logger LOGGER = Loggers.getLogger(Binding.class);

  private final int expectedSize;
  private final Map<Integer, BindValue> binds;

  public Binding(int expectedSize) {
    this.expectedSize = expectedSize;
    this.binds = new HashMap<>();
  }

  public Binding add(int index, BindValue parameter) {
    Assert.requireNonNull(parameter, "parameter must not be null");

    if (index >= this.expectedSize) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Binding index %d when only %d parameters are expected", index, this.expectedSize));
    }

    this.binds.put(index, parameter);

    return this;
  }

  public void clear() {
    this.binds
        .entrySet()
        .forEach(
            entry -> {
              Flux.from(entry.getValue().getValue())
                  .doOnNext(ReferenceCountUtil::release)
                  .subscribe(
                      ignore -> {},
                      err ->
                          LOGGER.warn(
                              String.format("Cannot release parameter %s", entry.getValue()), err));
            });

    this.binds.clear();
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
    return Objects.hash(this.binds);
  }

  public boolean isEmpty() {
    return this.binds.isEmpty();
  }

  public int size() {
    return this.binds.size();
  }

  @Override
  public String toString() {
    return "Binding{binds=" + this.binds + '}';
  }

  public void validate(int expectedSize) {
    // valid parameters
    for (int i = 0; i < expectedSize; i++) {
      if (binds.get(i) == null) {
        throw new IllegalStateException(String.format("Parameter at position %d is not set", i));
      }
    }
  }

  public List<BindValue> getBindResultParameters(int paramNumber) {
    if (this.binds.isEmpty() && paramNumber == 0) {
      return Collections.emptyList();
    }
    List<BindValue> result = new ArrayList<>(paramNumber);
    for (int i = 0; i < paramNumber; i++) {
      BindValue parameter = this.binds.get(i);
      if (parameter == null) {
        throw new IllegalStateException(String.format("No parameter specified for index %d", i));
      }
      result.add(parameter);
    }
    return result;
  }
}
