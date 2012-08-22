package org.ch.pump;

import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;

/** Author: duxbury */
public class Pump {
  private Pump() {

  }

  public static Pump prime() {
    return null;
  }

  public static Pump prime(Pipe pipe) {
    return null;
  }

  public Pump each(Function function, String... args) {
    return null;
  }

  public Pump each(Filter function, String... args) {
    return null;
  }

  public Pump groupby(String... fields) {
    return null;
  }

  public static Pump cogroup(Pump left, Pump right) {
    return null;
  }

  public static Pump cogroup(Pump left, Pump right, String... cogroupFields) {
    return null;
  }

  public static Pump cogroup(Pump left, Pump right, Joiner joiner, String... cogroupFields) {
    return null;
  }

  public Pump every(Aggregator agg, String... args) {
    return null;
  }

  public Pipe toPipe() {
    return null;
  }
}
