package org.ch.pump;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class FunctionPump extends InternalPump {
  private Function function;
  private String[] args;

  public FunctionPump(Pump prev, Function function, String[] args) {
    super(prev);
    this.function = function;
    this.args = args;
  }

  @Override Pipe toPipe() {
    return new Each(getPrev().toPipe(), getArgSelector(args), function, Fields.ALL);
  }
}
