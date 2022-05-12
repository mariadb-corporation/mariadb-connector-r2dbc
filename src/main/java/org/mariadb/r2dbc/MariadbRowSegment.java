// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.List;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class MariadbRowSegment extends MariadbReadable
    implements Result.RowSegment, Row, MariadbDataSegment {
  private MariadbRowMetadata meta = null;

  public MariadbRowSegment(RowDecoder decoder, List<ColumnDefinitionPacket> metadataList) {
    super(decoder, metadataList);
  }

  public void updateRaw(ByteBuf data) {
    super.updateRaw(data);
  }

  @Override
  public Row row() {
    return this;
  }

  @Override
  public RowMetadata getMetadata() {
    if (meta == null) meta = new MariadbRowMetadata(metadataList);
    return meta;
  }
}
