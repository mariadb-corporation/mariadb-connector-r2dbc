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

package org.mariadb.r2dbc.codec;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.util.BufferUtils;

public class Parameter<T> {
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static final Parameter<?> NULL_PARAMETER =
      new Parameter(null, null) {
        @Override
        public void encodeText(ByteBuf out, Context context) {
          BufferUtils.writeAscii(out, "null");
        }

        @Override
        public DataType getBinaryEncodeType() {
          return DataType.VARCHAR;
        }

        @Override
        public boolean isNull() {
          return true;
        }
      };

  private final Codec<T> codec;
  private final T value;

  public Parameter(Codec<T> codec, T value) {
    this.codec = codec;
    this.value = value;
  }

  public void encodeText(ByteBuf out, Context context) {
    codec.encodeText(out, context, this.value);
  }

  public void encodeBinary(ByteBuf out, Context context) {
    codec.encodeBinary(out, context, this.value);
  }

  public void encodeLongData(ByteBuf out, Context context) {
    codec.encodeBinary(out, context, this.value);
  }

  public DataType getBinaryEncodeType() {
    return codec.getBinaryEncodeType();
  }

  public boolean isNull() {
    return false;
  }

  @Override
  public String toString() {
    return "Parameter{codec=" + codec.getClass().getSimpleName() + ", value=" + value + '}';
  }
}
