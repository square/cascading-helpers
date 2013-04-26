package org.ch.function;

import cascading.tuple.Tuple;

public class TestGetOrElse extends FunctionHelper {
  public void testGetOrElse() throws Exception {
    final Tuple get = new Tuple("get");
    final Tuple orElse = new Tuple("else");
    GetOrElse goe = new GetOrElse(orElse, "field");

    assertEquals(get, operateFunc(goe, get));
    assertEquals(orElse, operateFunc(goe, new Tuple((Object)null)));
  }
}
