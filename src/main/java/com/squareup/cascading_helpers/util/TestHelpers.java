package com.squareup.cascading_helpers.util;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A set of helpers to test {@link cascading.operation.Function} and {@link cascading.operation.Filter}
 */
public final class TestHelpers {
  /* Utility. Don't instantiate. */
  private TestHelpers() {}

  public static List<Tuple> exec(Function f, Fields inputFields, final Tuple... input) {
    final List<Tuple> output = new ArrayList<Tuple>();
    FunctionCall mockFunctionCall = mock(FunctionCall.class);

    f.prepare(new HadoopFlowProcess(), null);

    for (final Tuple tuple : input) {
      final TupleEntry arg = new TupleEntry(inputFields, tuple);
      when(mockFunctionCall.getArguments()).thenReturn(arg);
      when(mockFunctionCall.getOutputCollector()).thenReturn(new TupleEntryCollector() {
        @Override protected void collect(TupleEntry tupleEntry) throws IOException {
          output.add(tupleEntry.getTuple());
        }
      });

      f.operate(new HadoopFlowProcess(), mockFunctionCall);
    }
    return output;
  }

  public static List<Boolean> exec(Filter f, Fields inputFields, final Tuple... input) {
    final List<Boolean> output = new ArrayList<Boolean>();
    FilterCall mockFilterCall = mock(FilterCall.class);

    f.prepare(new HadoopFlowProcess(), null);

    for (final Tuple t : input) {
      when(mockFilterCall.getArguments()).thenReturn(new TupleEntry(inputFields, t));
      output.add(f.isRemove(new HadoopFlowProcess(), mockFilterCall));
    }

    return output;
  }
}
