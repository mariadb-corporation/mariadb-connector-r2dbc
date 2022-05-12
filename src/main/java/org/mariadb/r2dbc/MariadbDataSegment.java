// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Result;

public interface MariadbDataSegment extends Result.Segment {

  void updateRaw(ByteBuf data);
}
