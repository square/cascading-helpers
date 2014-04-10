package com.squareup.cascading_helpers.pump;

import cascading.pipe.Pipe;

public class PipeAdapterPump extends Pump {
  private Pipe pipe;
  private Pump prev;

  public PipeAdapterPump(String name) {
    this(new Pipe(name));
  }

  public PipeAdapterPump(Pipe pipe) {
    this.pipe = pipe;
  }

  public PipeAdapterPump(Pump prev, Pipe pipe) {
    this.prev = prev;
    this.pipe = pipe;
  }

  @Override Pump getPrev() {
    return prev;
  }

  @Override public Pipe getPipeInternal() {
    return pipe;
  }
}
