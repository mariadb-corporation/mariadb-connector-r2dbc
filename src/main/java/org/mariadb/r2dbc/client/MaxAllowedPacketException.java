// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.r2dbc.spi.R2dbcNonTransientResourceException;

/**
 * Thrown when a command the driver is about to send is larger than the configured {@code
 * maxAllowedPacket}.
 *
 * <p>Nothing of the command has reached the socket when this is thrown, but the connection is
 * closed regardless: the command's response slot is already queued, so silently skipping the
 * command would desynchronize subsequent responses.
 */
public class MaxAllowedPacketException extends R2dbcNonTransientResourceException {

  public MaxAllowedPacketException(String reason) {
    super(reason, "08000");
  }
}
