package com.squareup.cascading_helpers.filter;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.squareup.cascading_helpers.TestHelpers;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FilterEqualsTest {
  @Test
  public void testSingle() throws Exception {
    List<Boolean> result = TestHelpers.exec(
        new FilterEquals(1),
        new Fields("blah"),
        new Tuple(1),
        new Tuple(2)
    );

    assertEquals(
        Arrays.asList(true, false),
        result);
  }

  @Test
  public void testMulti() throws Exception {
    List<Boolean> result = TestHelpers.exec(
        new FilterEquals(1, "two", true),
        new Fields("first", "second", "third"),
        new Tuple(1, "two", true),
        new Tuple(2, "two", true)
    );

    assertEquals(
        Arrays.asList(true, false),
        result);
  }
}
