package com.squareup.cascading_helpers.function;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.squareup.cascading_helpers.util.TestHelpers;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestMapLookup {
  private static final Map<Tuple,Tuple> SIMPLE_KEY_MAP = new HashMap<Tuple, Tuple>() {{
    put(new Tuple("first"), new Tuple("first result"));
    put(new Tuple("second"), new Tuple("second result"));
  }};

  private static final Map<Tuple, Tuple> COMPOUND_KEY_MAP = new HashMap<Tuple, Tuple>() {{
    put(new Tuple("first", "first prime"), new Tuple("first result", "plus some more"));
    put(new Tuple("second", "second prime"), new Tuple("second result", null));
  }};

  @Test
  public void testSimpleKey() throws Exception {
    List<Tuple> results = TestHelpers.exec(
        new MapLookup(SIMPLE_KEY_MAP, new Fields("value")),
        new Fields("blah"),
        new Tuple("first"),
        new Tuple("second"),
        new Tuple("not in the map"));

    assertEquals(Arrays.asList(
        new Tuple("first result"),
        new Tuple("second result"),
        new Tuple((Object)null)),
        results);
  }

  @Test
  public void testCompoundKey() throws Exception {
    List<Tuple> results = TestHelpers.exec(
        new MapLookup(COMPOUND_KEY_MAP, new Fields("value1", "value2")),
        new Fields("blah", "blahh"),
        new Tuple("first", "first prime"),
        new Tuple("second", "second prime"),
        new Tuple("not in the map", "lolcatz"));

    assertEquals(Arrays.asList(
        new Tuple("first result", "plus some more"),
        new Tuple("second result", null),
        new Tuple((Object)null, null)),
        results);
  }
}
