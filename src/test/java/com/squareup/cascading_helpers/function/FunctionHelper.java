package com.squareup.cascading_helpers.function;

import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;

public class FunctionHelper extends TestCase {
  @SuppressWarnings("unchecked")
  protected static Tuple operateFunc(final Function f, final Tuple argument) {
    final AtomicReference<Tuple> result = new AtomicReference<Tuple>();
    f.operate(null, new FunctionCall() {
      @Override public TupleEntry getArguments() {
        return new TupleEntry(f.getFieldDeclaration(), argument);
      }

      @Override public Fields getDeclaredFields() {
        return null;
      }

      @Override public TupleEntryCollector getOutputCollector() {
        TupleEntryCollector mockOutputCollector = new TupleEntryCollector() {
          @Override protected void collect(TupleEntry tupleEntry) throws IOException {
            result.set(tupleEntry.getTuple());
          }
        };
        return mockOutputCollector;
      }

      @Override public Object getContext() {
        return null;
      }

      @Override public void setContext(Object o) {}

      @Override public Fields getArgumentFields() {
        return null;
      }
    });
    return result.get();
  }
}
