/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.api;

import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

public interface MariadbStatement extends Statement {

  @Override
  MariadbStatement add();

  @Override
  MariadbStatement bind(String identifier, Object value);

  @Override
  MariadbStatement bind(int index, Object value);

  @Override
  MariadbStatement bindNull(String identifier, Class<?> type);

  @Override
  MariadbStatement bindNull(int index, Class<?> type);

  @Override
  Flux<MariadbResult> execute();

  @Override
  default MariadbStatement fetchSize(int rows) {
    return this;
  }

  @Override
  MariadbStatement returnGeneratedValues(String... columns);
}
