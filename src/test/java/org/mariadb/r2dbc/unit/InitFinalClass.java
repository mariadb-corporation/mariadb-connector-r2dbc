package org.mariadb.r2dbc.unit;

import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.codec.Codecs;

public class InitFinalClass {

  @Test
  public void init() throws Exception {
    Codecs codecs = new Codecs();
  }
}
