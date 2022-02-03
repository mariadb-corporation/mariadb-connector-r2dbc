package org.mariadb.r2dbc.codec;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Type;

public class ParameterWithCodec implements Parameter {
  Parameter param;
  Codec<?> codec;

  public ParameterWithCodec(Parameter param, Codec<?> codec) {
    this.param = param;
    this.codec = codec;
  }

  @Override
  public Type getType() {
    return param.getType();
  }

  @Override
  public Object getValue() {
    return param.getValue();
  }

  public Codec<?> getCodec() {
    return codec;
  }

  @Override
  public String toString() {
    return "ParameterWithCodec{"
        + "param="
        + param
        + ", codec="
        + codec.getClass().getSimpleName()
        + '}';
  }
}
