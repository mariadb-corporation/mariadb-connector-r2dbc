// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.buffer.ByteBuf;
import java.util.Objects;
import org.mariadb.r2dbc.codec.Codec;
import reactor.core.publisher.Mono;

public class BindValue {

  public static final Mono<? extends ByteBuf> NULL_VALUE = Mono.empty();
  private final Codec<?> codec;
  private final Mono<? extends ByteBuf> value;

  public BindValue(Codec<?> codec, Mono<? extends ByteBuf> value) {
    this.codec = codec;
    this.value = Assert.requireNonNull(value, "value must not be null");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BindValue that = (BindValue) o;
    return Objects.equals(this.codec, that.codec) && Objects.equals(this.value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.codec, this.value);
  }

  @Override
  public String toString() {
    return "BindValue{codec=" + this.codec.getClass().getSimpleName() + '}';
  }

  public Codec<?> getCodec() {
    return this.codec;
  }

  public boolean isNull() {
    return this.value == NULL_VALUE;
  }

  public Mono<? extends ByteBuf> getValue() {
    return this.value;
  }
}
