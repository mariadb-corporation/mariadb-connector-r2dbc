// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.*;
import java.util.List;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class MariadbOutSegment extends MariadbReadable
    implements Result.OutSegment, OutParameters, MariadbDataSegment {

  public MariadbOutSegment(RowDecoder decoder, List<ColumnDefinitionPacket> metadataList) {
    super(decoder, metadataList);
  }

  public void updateRaw(ByteBuf data) {
    super.updateRaw(data);
  }

  @Override
  public OutParameters outParameters() {
    return this;
  }

  @Override
  public OutParametersMetadata getMetadata() {
    return new MariadbOutParametersMetadata(metadataList);
  }
}
