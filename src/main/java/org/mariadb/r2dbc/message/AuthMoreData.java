// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

import io.netty.buffer.ByteBuf;

public interface AuthMoreData {

  MessageSequence getSequencer();

  ByteBuf getBuf();
}
