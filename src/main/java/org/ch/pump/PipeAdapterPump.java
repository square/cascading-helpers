package org.ch.pump;

import cascading.pipe.Pipe;

public class PipeAdapterPump extends Pump {
  private Pipe pipe;

  public PipeAdapterPump(String name) {
    this(new Pipe(name));
  }

  public PipeAdapterPump(Pipe pipe) {
    this.pipe = pipe;
  }

  @Override Pump getPrev() {
    return null;
  }

  @Override public Pipe getPipeInternal() {
    return pipe;
  }
}
