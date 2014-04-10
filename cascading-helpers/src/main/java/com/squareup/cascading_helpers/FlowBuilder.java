package com.squareup.cascading_helpers;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowListener;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.squareup.cascading_helpers.pump.Pump;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FlowBuilder is a fluent interface extending FlowDef to better integrate with
 * the {@link com.squareup.cascading_helpers.operation.KnowsEmittedClasses} interface. It is meant
 * to assist in declaratively building a FlowDef, and returns the {@link cascading.flow.Flow} upon
 * calling the {@link #build()} method.
 */
public class FlowBuilder {
  private final FlowDef flowDef;
  private final Set<Class> emittedClasses;
  private final List<FlowListener> listeners;
  private final Map<Object, Object> properties;

  protected FlowBuilder() {
    this.flowDef = new FlowDef();
    this.emittedClasses = new HashSet<Class>();
    this.listeners = new ArrayList<FlowListener>();
    this.properties = new HashMap<Object, Object>();
  }

  public FlowBuilder hfsTextLineTailSink(Pump pump, String path, SinkMode mode) {
    emittedClasses.addAll(pump.getEmittedClasses());
    tailSink(pump, new Hfs(new TextLine(), path, mode));
    return this;
  }

  public FlowBuilder tailSink(Pump pump, Tap tap) {
    emittedClasses.addAll(pump.getEmittedClasses());
    flowDef.addTailSink(pump.toPipe(), tap);
    return this;
  }

  public FlowBuilder sink(Pump pump, Tap tap) {
    emittedClasses.addAll(pump.getEmittedClasses());
    flowDef.addSink(pump.toPipe(), tap);
    return this;
  }

  public FlowBuilder source(String name, Tap tap) {
    Pump pump = Pump.prime(name);
    flowDef.addSource(pump.toPipe(), tap);
    return this;
  }

  public FlowBuilder source(Pump pump, Tap tap) {
    flowDef.addSource(pump.toPipe(), tap);
    return this;
  }

  public FlowBuilder sources(Map<String, Tap> sources) {
    flowDef.addSources(sources);
    return this;
  }

  public FlowBuilder trap(Pump pump, Tap trap) {
    emittedClasses.addAll(pump.getEmittedClasses());
    flowDef.addTrap(pump.toPipe(), trap);
    return this;
  }

  public FlowBuilder name(String name) {
    flowDef.setName(name);
    return this;
  }

  /**
   * List of {@link cascading.flow.FlowListener} to be attached to the {@link cascading.flow.Flow}.
   * @param listeners - {@link java.util.List} of {@link cascading.flow.FlowListener}s.
   * @return {@link com.squareup.cascading_helpers.FlowBuilder}.
   */
  public FlowBuilder listeners(List<FlowListener> listeners) {
    this.listeners.addAll(listeners);
    return this;
  }

  /**
   * Map of underlying properties applied to the {@link cascading.flow.FlowConnector} for the
   * cascading job being build.
   * @param properties - {@link java.util.Map} of properties for the underlying
   * {@link cascading.flow.hadoop.HadoopFlowConnector}
   * @return {@link com.squareup.cascading_helpers.FlowBuilder}.
   */
  public FlowBuilder properties(Map<Object, Object> properties) {
    this.properties.putAll(properties);
    return this;
  }

  /**
   * Builds the underlying {@link cascading.flow.FlowDef} by populating the emittedClasses that
   * each tail sink knows of, attaching any provided {@link #properties(java.util.Map)} and
   * {@link #listeners(java.util.List)}, and returning the {@link cascading.flow.Flow}.
   * @return {@link cascading.flow.Flow}.
   */
  public Flow build() {
    CascadingHelper helper = CascadingHelper.get().withTokensFor(emittedClasses);
    FlowConnector connector;
    if (!properties.isEmpty()) {
      connector = helper.getFlowConnector(properties);
    } else {
      connector = helper.getFlowConnector();
    }

    Flow flow = connector.connect(flowDef);
    if (!listeners.isEmpty()) {
      for (FlowListener l : listeners) {
        flow.addListener(l);
      }
    }
    return flow;
  }

  /**
   * Get a handle to the current {@link #flowDef}. Use discouraged.
   * @return
   */
  public FlowDef getFlowDef() {
    return this.flowDef;
  }

  public Set<Class> getEmittedClasses() {
    return emittedClasses;
  }
}
