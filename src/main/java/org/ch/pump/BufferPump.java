package org.ch.pump;

import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class BufferPump extends EveryPump {
  private final Buffer buffer;

  public BufferPump(Pump prev, Buffer buffer, String[] args) {
    super(prev, args);
    this.buffer = buffer;
  }

  @Override public Pipe getPipeInternal() {
    return new Every(getPrev().toPipe(), getArgSelector(Fields.VALUES, args), buffer);
  }
}
