package org.ch.pump;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import java.util.HashSet;
import java.util.Set;
import org.ch.function.StacktraceWrapperFunction;
import org.ch.operation.KnowsEmittedClasses;

public class FunctionPump extends InternalPump {
  private Function function;
  private String[] args;

  public FunctionPump(Pump prev, Function function, String[] args) {
    super(prev);
    this.function = function;
    this.args = args;
  }

  @Override public Pipe getPipeInternal() {
    return new Each(getPrev().toPipe(), getArgSelector(args), new StacktraceWrapperFunction(function, getStackTrace()), Fields.ALL);
  }

  @Override public Set<Class> getEmittedClasses() {
    Set<Class> combined = new HashSet<Class>();

    if (function instanceof KnowsEmittedClasses) {
      Set<Class> emittedClasses = ((KnowsEmittedClasses)function).getEmittedClasses();
      combined.addAll(emittedClasses);
    }
    combined.addAll(super.getEmittedClasses());
    return combined;
  }
}
