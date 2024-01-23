// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.RowMetadata;
import org.mariadb.r2dbc.client.MariadbOutParametersMetadata;

/** A {@link io.r2dbc.spi.OutParameters} for a MariaDB/MySQL database. */
public interface MariadbOutParameters extends OutParameters {

  /**
   * Returns the {@link RowMetadata} for all columns in this row.
   *
   * @return the {@link RowMetadata} for all columns in this row
   * @since 0.9
   */
  MariadbOutParametersMetadata getMetadata();
}
