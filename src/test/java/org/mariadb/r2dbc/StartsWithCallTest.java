// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the stored-procedure CALL detector used to force the binary protocol. Runs without
 * a database (the detector is pure, allocation-free string scanning).
 */
public class StartsWithCallTest {

  @Test
  void detectsCall() {
    assertTrue(MariadbConnection.startsWithCall("call proc()"));
    assertTrue(MariadbConnection.startsWithCall("CALL proc()"));
    assertTrue(MariadbConnection.startsWithCall("Call proc()"));
    assertTrue(MariadbConnection.startsWithCall("  call proc()")); // leading spaces
    assertTrue(MariadbConnection.startsWithCall("\t\ncall proc()")); // leading tab/newline
    assertTrue(MariadbConnection.startsWithCall("CALL\tproc()")); // tab after keyword
    assertTrue(MariadbConnection.startsWithCall("/* hint */ CALL proc()")); // block comment
    assertTrue(MariadbConnection.startsWithCall("/* a */ /* b */ call proc()"));
    assertTrue(MariadbConnection.startsWithCall("# comment\ncall proc()")); // # line comment
    assertTrue(MariadbConnection.startsWithCall("-- comment\nCALL proc()")); // -- line comment
  }

  @Test
  void rejectsNonCall() {
    assertFalse(MariadbConnection.startsWithCall("SELECT 1 as caller")); // substring, not prefix
    assertFalse(MariadbConnection.startsWithCall("SELECT callback FROM t"));
    assertFalse(MariadbConnection.startsWithCall("callback()")); // CALL not followed by whitespace
    assertFalse(MariadbConnection.startsWithCall("CALLproc()"));
    assertFalse(MariadbConnection.startsWithCall("call")); // keyword only, no procedure
    assertFalse(MariadbConnection.startsWithCall("INSERT INTO t VALUES (1)"));
  }

  @Test
  void doubleDashIsCommentOnlyWhenFollowedByWhitespace() {
    // "-- " (whitespace after) is a comment -> the CALL on the next line is detected
    assertTrue(MariadbConnection.startsWithCall("-- x\nCALL proc()"));
    // "--x" (no whitespace) is NOT a comment -> statement starts with '-', not CALL
    assertFalse(MariadbConnection.startsWithCall("--x\nCALL proc()"));
  }

  @Test
  void unclosedBlockCommentIsNotCall() {
    assertFalse(MariadbConnection.startsWithCall("/* never closed CALL proc()"));
  }
}
