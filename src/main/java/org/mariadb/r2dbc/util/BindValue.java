// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Objects;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.message.Context;
import reactor.core.publisher.Mono;

public class BindValue {

  private final Codec<?> codec;
  private final Object value;

  public BindValue(Codec<?> codec, Object value) {
    this.codec = codec;
    this.value = value;
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
    return this.value == null;
  }

  public void encodeDirectText(ByteBuf out, Context context) {
    this.codec.encodeDirectText(out, this.value, context);
  }

  public void encodeDirectBinary(ByteBufAllocator allocator, ByteBuf out, Context context) {
    this.codec.encodeDirectBinary(allocator, out, this.value, context);
  }

  public Mono<ByteBuf> encodeText(ByteBufAllocator allocator, Context context) {
    return this.codec.encodeText(allocator, this.value, context);
  }

  public Mono<ByteBuf> encodeBinary(ByteBufAllocator allocator) {
    return this.codec.encodeBinary(allocator, this.value);
  }

  public Object getValue() {
    return this.value;
  }
}
