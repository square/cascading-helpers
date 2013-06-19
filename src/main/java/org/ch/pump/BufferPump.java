package org.ch.pump;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.Pipe;

public class BufferPump extends EveryPump {
  private final Buffer buffer;

  public BufferPump(Pump prev, Buffer buffer, String[] args) {
    super(prev, args);
    this.buffer = buffer;
  }

  @Override public Pipe getPipeInternal() {
    return new Every(getPrev().toPipe(), getArgSelector(args), buffer);
  }
}
