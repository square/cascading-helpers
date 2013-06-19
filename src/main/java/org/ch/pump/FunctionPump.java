package org.ch.pump;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import org.ch.function.StacktraceWrapperFunction;

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
}
