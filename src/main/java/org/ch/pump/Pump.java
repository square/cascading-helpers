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
import java.util.Arrays;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class Pump {
  abstract Pump getPrev();
  abstract Pipe toPipe();

  public static Pump prime() {
    return prime("input");
  }

  public static Pump prime(String pipeName) {
    return new PipeAdapterPump(pipeName);
  }

  public static Pump prime(Pipe pipe) {
    return new PipeAdapterPump(pipe);
  }

  public Pump cogroup(Pump other, String... cogroupFields) {
	  return cogroup(this, other, cogroupFields);
  }
  
  public Pump cogroup(Pump other, Joiner joiner, String... cogroupFields) {
	  return cogroup(this, other, joiner, cogroupFields);
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

    return new PipeAdapterPump(new CoGroup(left.toPipe(), getArgSelector(cogroupFields), right.toPipe(), getArgSelector(modifiedCogroupFields), joiner));
  }

  static Fields getArgSelector(String... args) {
    Fields f = Fields.ALL;
    if (args.length > 0) {
      f = new Fields(args);
    }
    return f;
  }

  public Pump each(Function function, String... args) {
    return new FunctionPump(this, function, args);
  }

  public Pump each(Filter filter, String... args) {
    return new FilterPump(this, filter, args);
  }

  public Pump unique(String... uniqueFields) {
    return new PipeAdapterPump(new Unique(toPipe(), getArgSelector(uniqueFields)));
  }

  public Pump groupby(String... fields) {
    return new PipeAdapterPump(new GroupBy(toPipe(), getArgSelector(fields)));
  }

  public Pump every(Aggregator agg, String... args) {
    return new EveryPump(this, agg, args);
  }

  public Pump retain(String ... fieldsToKeep) {
    return new PipeAdapterPump(new Retain(toPipe(), getArgSelector(fieldsToKeep)));
  }

  public Pump discard(String ... fieldsToDiscard) {
    return new PipeAdapterPump(new Discard(toPipe(), getArgSelector(fieldsToDiscard)));
  }

  public Pump coerce(String field, Class toClass) {
    return coerce(toClass, field);
  }

  public Pump coerce(Class toClass, String... fieldsToCoerce) {
    Class<?>[] classes = new Class<?>[fieldsToCoerce.length];
    Arrays.fill(classes, toClass);
    return coerce(fieldsToCoerce, classes);
  }

  public Pump coerce(String[] fields, Class<?>[] classes) {
    return new PipeAdapterPump(new Coerce(toPipe(), new Fields(fields), classes));
  }

  public Pump rename(String field, String toName) {
    return new PipeAdapterPump(new Rename(toPipe(), new Fields(field), new Fields(toName)));
  }

  public Pump replace(String field, String toName) {
    return discard(toName).rename(field, toName);
  }
}
