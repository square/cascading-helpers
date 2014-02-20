package com.squareup.cascading_helpers.function;

import cascading.tuple.Tuple;

public class TestGetOrElse extends FunctionHelper {
  public void testGetOrElse() throws Exception {
    final Tuple get = new Tuple("get");
    final Tuple orElse = new Tuple("else");
    GetOrElse goe = new GetOrElse(orElse, "field");

    assertEquals(get, operateFunc(goe, get));
    assertEquals(orElse, operateFunc(goe, new Tuple((Object)null)));
  }
  public void testGOETupleSize() throws Exception {
    try {
      final Tuple orElse = new Tuple("else", "b");
      GetOrElse goe = new GetOrElse(orElse, "field");
    } catch (IllegalArgumentException iae) {
      return;
    }
    fail("did not raise exception");
  }
}
