package org.ch.pump;

import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

/** Author: duxbury */
public class Pump {
  private final Pipe prev;

  private Pump(String name) {
    this.prev = new Pipe(name);
  }

  private Pump(Pipe prev) {
    this.prev = prev;
  }

  public static Pump prime() {
    return prime("input");
  }

  public static Pump prime(String pipeName) {
    return new Pump(pipeName);
  }

  public static Pump prime(Pipe pipe) {
    return new Pump(pipe);
  }

  public static Pump cogroup(Pump left, Pump right, String... cogroupFields) {
    return cogroup(left, right, new InnerJoin(), cogroupFields);
  }

  public static Pump cogroup(Pump left, Pump right, Joiner joiner, String... cogroupFields) {
    String[] modifiedCogroupFields = new String[cogroupFields.length];
    for (int i = 0; i < cogroupFields.length; i++) {
      String cogroupField = cogroupFields[i];
      String modifieldField = "__rhs__" + cogroupField;
      modifiedCogroupFields[i] = modifieldField;
      right = right.rename(cogroupField, modifieldField);
    }

    return new Pump(new CoGroup(left.toPipe(), getArgSelector(cogroupFields), right.toPipe(), getArgSelector(modifiedCogroupFields), joiner));
  }

  private static Fields getArgSelector(String... args) {
    Fields f = Fields.ALL;
    if (args.length > 0) {
      f = new Fields(args);
    }
    return f;
  }

  public Pump each(Function function, String... args) {
    return new Pump(new Each(prev, getArgSelector(args), function, Fields.ALL));
  }

  public Pump each(Filter filter, String... args) {
    return new Pump(new Each(prev, getArgSelector(args), filter));
  }

  public Pump unique(String... uniqueFields) {
    return new Pump(new Unique(prev, getArgSelector(uniqueFields)));
  }

  public Pump groupby(String... fields) {
    return new Pump(new GroupBy(prev, getArgSelector(fields)));
  }

  public Pump every(Aggregator agg, String... args) {
    return new Pump(new Every(prev, getArgSelector(args), agg));
  }

  public Pipe toPipe() {
    return prev;
  }

  public Pump retain(String ... fieldsToKeep) {
    return new Pump(new Retain(prev, getArgSelector(fieldsToKeep)));
  }

  public Pump discard(String ... fieldsToDiscard) {
    return new Pump(new Discard(prev, getArgSelector(fieldsToDiscard)));
  }

  public Pump coerce(String field, Class toClass) {
    return new Pump(new Coerce(prev, new Fields(field), toClass));
  }

  public Pump rename(String field, String toName) {
    return new Pump(new Rename(prev, new Fields(field), new Fields(toName)));
  }
}
