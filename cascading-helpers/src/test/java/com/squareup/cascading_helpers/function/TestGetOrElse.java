package com.squareup.cascading_helpers.function;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.squareup.cascading_helpers.TestHelpers;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestGetOrElse  {
  @Test
  public void testGetOrElse() throws Exception {
    final Tuple get = new Tuple("get");
    final Tuple orElse = new Tuple("else");

    List<Tuple> results = TestHelpers.exec(
        new GetOrElse(orElse, "field"),
        new Fields("blah"),
        new Tuple(get),
        new Tuple((Object)null));

    assertEquals(Arrays.asList(new Tuple(get), new Tuple(orElse)), results);
  }

  @Test
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
