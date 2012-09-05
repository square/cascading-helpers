package org.ch.function;

import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;

/** Author: duxbury */
public class TestMapLookup extends TestCase {
  private static final Map<Tuple,Tuple> SIMPLE_KEY_MAP = new HashMap<Tuple, Tuple>() {{
    put(new Tuple("first"), new Tuple("first result"));
    put(new Tuple("second"), new Tuple("second result"));
  }};

  private static final Map<Tuple, Tuple> COMPOUND_KEY_MAP = new HashMap<Tuple, Tuple>() {{
    put(new Tuple("first", "first prime"), new Tuple("first result", "plus some more"));
    put(new Tuple("second", "second prime"), new Tuple("second result", null));
  }};

  public void testSimpleKey() throws Exception {
    MapLookup func = new MapLookup(SIMPLE_KEY_MAP, new Fields("value"));
    assertEquals(new Tuple("first result"), operateFunc(func, new Tuple("first")));
    assertEquals(new Tuple("second result"), operateFunc(func, new Tuple("second")));
    assertEquals(new Tuple((Object)null), operateFunc(func, new Tuple("not in the map")));
  }

  public void testCompoundKey() throws Exception {
    MapLookup func = new MapLookup(COMPOUND_KEY_MAP, new Fields("value1", "value2"));
    assertEquals(new Tuple("first result", "plus some more"), operateFunc(func, new Tuple("first", "first prime")));
    assertEquals(new Tuple("second result", null), operateFunc(func, new Tuple("second", "second prime")));
    assertEquals(new Tuple((Object)null, null), operateFunc(func, new Tuple("not in the map", "srsly")));
  }

  protected Tuple operateFunc(final MapLookup f, final Tuple argument) {
    final AtomicReference<Tuple> result = new AtomicReference<Tuple>();
    f.operate(null, new FunctionCall() {
      @Override public TupleEntry getArguments() {
        return new TupleEntry(f.getFieldDeclaration(), argument);
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
