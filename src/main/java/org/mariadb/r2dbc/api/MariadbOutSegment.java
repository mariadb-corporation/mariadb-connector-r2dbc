package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;

public interface MariadbOutSegment extends Result.OutSegment {
  Row row();
}
