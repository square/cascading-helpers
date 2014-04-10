package com.squareup.cascading_helpers.pump;

public abstract class EveryPump extends InternalPump {
  protected final String[] args;

  public EveryPump(Pump prev, String[] args) {
    super(prev);
    this.args = args;
  }

  //@Override Pipe toPipe() {
  //  return new Every(getPrev().toPipe(), getArgSelector(args), agg);
  //}
}
