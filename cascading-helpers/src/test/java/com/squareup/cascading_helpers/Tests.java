package com.squareup.cascading_helpers;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.FunctionCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.squareup.cascading_helpers.operation.KnowsEmittedClasses;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Tests {
  public static final String INPUT_PATH = "/tmp/TestPump/input";
  public static final String TRAP_PATH = "/tmp/TestPump/trap";
  public static final String OUTPUT_PATH = "/tmp/TestPump/output";
  public static final List<Tuple> INPUT_TUPLES = new ArrayList<Tuple>(){{
    add(new Tuple("115200000"));
    add(new Tuple("0"));
    add(new Tuple("115200000"));
    add(new Tuple("asdf"));
  }};

  @SuppressWarnings({"unchecked"})
  public static void fillTap(List<Tuple> tuples, Tap tap) throws IOException {
    TupleEntryCollector tec = tap.openForWrite(new HadoopFlowProcess());
    for (Tuple t : tuples) {
      tec.add(new TupleEntry(new Fields("line"), t));
    }
    tec.close();
  }

  public static Tap getTap(String path) {
    return new Hfs(new TextLine(), path);
  }

  public static Tap getInTap() {
    return getTap(INPUT_PATH);
  }

  public static Tap getOutTap() {
    return getTap(OUTPUT_PATH);
  }

  public static Tap getTrap() {
    return getTap(TRAP_PATH);
  }

  public static class Left {}

  public static class Right {}

  public static class FunctionThatKnows extends BaseOperation implements KnowsEmittedClasses {
    private final Class klass;

    public FunctionThatKnows(Class klass) {
      this.klass = klass;
    }

    @Override public Set<Class> getEmittedClasses() {
      return Collections.singleton(klass);
    }
    @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    }
  }
}
