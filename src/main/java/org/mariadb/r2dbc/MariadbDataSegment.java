package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Result;

public interface MariadbDataSegment extends Result.Segment {

  void updateRaw(ByteBuf data);
}
