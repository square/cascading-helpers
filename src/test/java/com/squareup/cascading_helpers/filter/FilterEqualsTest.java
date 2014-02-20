package com.squareup.cascading_helpers.filter;

import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterEqualsTest {
  @Test
  public void testSingle() throws Exception {
    assertTrue(operateFilter(new FilterEquals(1), new TupleEntry(new Fields("blah"), new Tuple(1))));
    assertFalse(
        operateFilter(new FilterEquals(2), new TupleEntry(new Fields("blah"), new Tuple(1))));
  }

  @Test
  public void testMulti() throws Exception {
    assertTrue(operateFilter(new FilterEquals(1, "two", true), new TupleEntry(new Fields("first", "second", "third"), new Tuple(1, "two", true))));
    assertFalse(
        operateFilter(new FilterEquals(1, "two", true), new TupleEntry(new Fields("first", "second", "third"), new Tuple(2, "two", true))));
  }

  private boolean operateFilter(Filter filter, final TupleEntry args) {
    return filter.isRemove(null, new FilterCall() {

      @Override public TupleEntry getArguments() {
        return args;
      }

      @Override public Object getContext() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override public void setContext(Object o) {
        //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override public Fields getArgumentFields() {
        return args.getFields();
      }
    });
  }
}
