/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.mariadb.r2dbc.client.MariadbRowMetadata;

/** A {@link Row} for a MariaDB/MySQL database. */
public interface MariadbRow extends Row {

  /**
   * Returns the {@link RowMetadata} for all columns in this row.
   *
   * @return the {@link RowMetadata} for all columns in this row
   * @since 0.9
   */
  MariadbRowMetadata getMetadata();
}
