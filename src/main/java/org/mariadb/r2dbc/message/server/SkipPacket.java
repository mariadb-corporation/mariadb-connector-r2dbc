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

package org.mariadb.r2dbc.message.server;

public class SkipPacket implements ServerMessage {

  private final boolean ending;

  public SkipPacket(boolean ending) {
    this.ending = ending;
  }

  public static SkipPacket decode(boolean ending) {
    return new SkipPacket(ending);
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  public Sequencer getSequencer() {
    return null;
  }

}
