package org.ch.pump;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Max;
import cascading.operation.aggregator.Sum;
import cascading.operation.filter.FilterNull;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.DateFormatter;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ch.CascadingHelper;
import org.ch.operation.KnowsEmittedClasses;

public class TestPump extends TestCase {

  private static final String INPUT_PATH = "/tmp/TestPump/input";
  private static final String INPUT2_PATH = "/tmp/TestPump/input2";
  private static final String OUTPUT_PATH = "/tmp/TestPump/output";
  private static final String OUTPUT_PATH2 = "/tmp/TestPump/output2";

  private static final List<Tuple> INPUT_TUPLES = new ArrayList<Tuple>(){{
    add(new Tuple("115200000"));
    add(new Tuple("0"));
    add(new Tuple("115200000"));
    add(new Tuple("asdf"));
  }};

  private static final List<Tuple> INPUT2_TUPLES = new ArrayList<Tuple>(){{
    add(new Tuple("1970-01-01\tfirst"));
    add(new Tuple("1970-01-02\tsecond"));
    add(new Tuple("1970-01-03\tfiltered"));
  }};

  public void setUp() throws Exception {
    CascadingHelper.setTestMode();
    FileSystem.get(new Configuration()).delete(new Path(INPUT_PATH), true);
    FileSystem.get(new Configuration()).delete(new Path(INPUT2_PATH), true);
    FileSystem.get(new Configuration()).delete(new Path(OUTPUT_PATH), true);
    FileSystem.get(new Configuration()).delete(new Path(OUTPUT_PATH2), true);

    Tap inTap = getInTap();
    TupleEntryCollector tec = inTap.openForWrite(new HadoopFlowProcess());
    for (Tuple t : INPUT_TUPLES) {
      tec.add(new TupleEntry(t));
    }
    tec.close();

    inTap = getIn2Tap();
    tec = inTap.openForWrite(new HadoopFlowProcess());
    for (Tuple t : INPUT2_TUPLES) {
      tec.add(new TupleEntry(t));
    }
    tec.close();
  }

  public Tap getInTap() {
    return new Hfs(new TextLine(), INPUT_PATH);
  }

  public Tap getIn2Tap() {
    return new Hfs(new TextLine(), INPUT2_PATH);
  }

  public Tap getOutTap() {
    return new Hfs(new TextLine(), OUTPUT_PATH);
  }

  public Tap getOutTap2() {
    return new Hfs(new TextLine(), OUTPUT_PATH2);
  }

  public void testRetain() throws IOException {
    Pipe p = Pump.prime()
        .retain("line")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("115200000", "0", "115200000", "asdf"), getOutputStrings());
  }

  public void testDiscard() throws IOException {
    Pipe p = Pump.prime()
        .discard("offset")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("115200000", "0", "115200000", "asdf"), getOutputStrings());
  }

  public void testReplace() throws Exception {
    Pipe p = Pump.prime()
        .replace("offset", "line")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("0", "10", "12", "22"), getOutputStrings());
  }

  // this is a pretty weak test, since the results are going to get stringified anyays
  public void testCoerce() throws Exception {
    Pipe p = Pump.prime()
        .discard("offset")
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .coerce("line", long.class)
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("115200000", "0", "115200000"), getOutputStrings());
  }

  public void testPrimeWithPipe() throws Exception {
    Pipe pipe = new Pipe("input");
    Pipe p = Pump.prime(pipe).retain("line").toPipe();
    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("115200000", "0", "115200000", "asdf"), getOutputStrings());
  }

  public void testEachFilter() throws IOException {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("115200000", "0", "115200000"), getOutputStrings());
  }

  public void testEachFunction() throws IOException {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("1970-01-02", "1970-01-01", "1970-01-02"), getOutputStrings());
  }

  public void testGroupBy() throws Exception {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .groupby("date")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("1970-01-01", "1970-01-02", "1970-01-02"), getOutputStrings());
  }

  public void testAggregator() throws Exception {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .groupby("date")
        .every(new Count(new Fields("count")))
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("1970-01-01\t1", "1970-01-02\t2"), getOutputStrings());
  }

  public void testBuffer() throws Exception {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .each(new Insert(new Fields("key"), 1))
        .groupby("key")
        .secondarySort("date")
        .every(new BufferFirst(), "date")
        .retain("date2")
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("1970-01-01"), getOutputStrings());
  }

  public void testCoGroup() throws Exception {
    Pump left = Pump.prime("left")
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .groupby("date")
        .every(new Count(new Fields("count")));
    Pump right = Pump.prime("right")
        .each(new RegexSplitter(new Fields("date", "tag"),"\t"), "line");

    Pipe pipe = Pump.cogroup(left, right, "date")
        .retain("date", "count", "tag")
        .toPipe();

    Map<String, Tap> inputTaps = new HashMap<String, Tap>() {{
      put("left", getInTap());
      put("right", getIn2Tap());
    }};

    CascadingHelper.get().getFlowConnector().connect(inputTaps, getOutTap(), pipe).complete();

    assertEquals(Arrays.asList("1970-01-01\t1\tfirst", "1970-01-02\t2\tsecond"), getOutputStrings());
  }

  public void testCoGroupEquality() {
	  Pump left = Pump.prime("left")
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .groupby("date")
        .every(new Count(new Fields("count")));
    Pump right = Pump.prime("right")
        .each(new RegexSplitter(new Fields("date", "tag"),"\t"), "line");

    CoGroup nonStaticPump = (CoGroup)left.cogroup(right, "date").toPipe();
    CoGroup staticPump = (CoGroup)Pump.cogroup(left, right, "date").toPipe();
    assertEquals(nonStaticPump.toString(), staticPump.toString());
    
    // NOTE: Since coGroup includes a rename internally, there is no way these two
    // arrays will ever be equal, since Pipe's equal checks object identity. The
    // left side is unmodified though, and can be checked for equality.
    Pipe[] nonStaticHeads = nonStaticPump.getPrevious();
    Pipe[] staticHeads = staticPump.getPrevious();
    assertEquals(2, nonStaticHeads.length);
    assertEquals(2, staticHeads.length);
    assertEquals(nonStaticHeads[0].toString(), staticHeads[0].toString());
    assertEquals(nonStaticHeads[1].toString(), staticHeads[1].toString());
  }

  public void testGroupBySecondarySort() throws IOException {
    String inputPath = "/tmp/TestPump/group_by_sec_sort";
    FileSystem.get(new Configuration()).delete(new Path(inputPath), true);

    Tap inTap = new Hfs(new SequenceFile(new Fields("key1", "key2")), inputPath);
    TupleEntryCollector collector = inTap.openForWrite(new HadoopFlowProcess());
    collector.add(new Tuple("key1", "value2"));
    collector.add(new Tuple("key1", "value1"));
    collector.close();

    Pump pump = Pump.prime()
        .retain("key1", "key2")
        .groupby("key1")
        .secondarySort("key2")
        .every(new First(new Fields("key11", "key21")), "key1", "key2");
        //.every(new DebugAggregator(), "key1", "key2");
    Pipe tail = pump.toPipe();

    FlowDef flowDef = new FlowDef()
        .addSource("input", inTap)
        .addTail(tail)
        .addSink(tail, new Hfs(new TextLine(new Fields("key1"), new Fields("key11", "key21")), OUTPUT_PATH));

    CascadingHelper.get().getFlowConnector().connect(flowDef).complete();
    List<String> outputStrings = getOutputStrings();
    assertEquals(Arrays.asList("key1\tvalue1"), outputStrings);
  }

  public void testGroupBySecondarySortReversed() throws IOException {
    String inputPath = "/tmp/TestPump/group_by_sec_sort";
    FileSystem.get(new Configuration()).delete(new Path(inputPath), true);

    Tap inTap = new Hfs(new SequenceFile(new Fields("key1", "key2")), inputPath);
    TupleEntryCollector collector = inTap.openForWrite(new HadoopFlowProcess());
    collector.add(new Tuple("key1", "value1"));
    collector.add(new Tuple("key1", "value2"));
    collector.close();

    Pump pump = Pump.prime()
        .retain("key1", "key2")
        .groupby("key1")
        .secondarySort("key2")
        .inReverse()
        .every(new First(new Fields("key11", "key21")), "key1", "key2");
    Pipe tail = pump.toPipe();

    FlowDef flowDef = new FlowDef()
        .addSource("input", inTap)
        .addTail(tail)
        .addSink(tail, new Hfs(new TextLine(new Fields("key1"), new Fields("key11", "key21")), OUTPUT_PATH));

    CascadingHelper.get().getFlowConnector().connect(flowDef).complete();
    List<String> outputStrings = getOutputStrings();
    assertEquals(Arrays.asList("key1\tvalue2"), outputStrings);
  }

  /**
   * Test calling Pump#every with no arguments.
   * (Relies on AggregatorPump correctly setting the default arguments.)
   */
  public void testEvery() throws IOException {
    String inputPath = "/tmp/TestPump/group_by_sec_sort";
    FileSystem.get(new Configuration()).delete(new Path(inputPath), true);

    Tap inTap = new Hfs(new SequenceFile(new Fields("key1", "key2")), inputPath);
    TupleEntryCollector collector = inTap.openForWrite(new HadoopFlowProcess());
    collector.add(new Tuple("key1", "value1"));
    collector.add(new Tuple("key1", "value2"));
    collector.close();

    Pump pump = Pump.prime()
        .retain("key1", "key2")
        .groupby("key1")
        .every(new First());
    Pipe tail = pump.toPipe();

    FlowDef flowDef = new FlowDef()
        .addSource("input", inTap)
        .addTail(tail)
        .addSink(tail, new Hfs(new TextLine(new Fields("key1")), OUTPUT_PATH));

    CascadingHelper.get().getFlowConnector().connect(flowDef).complete();
    List<String> outputStrings = getOutputStrings();
    assertEquals(Arrays.asList("key1\tvalue1"), outputStrings);
  }

  public void testUnique() throws Exception {
    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), Pump.prime().retain("line").unique("line").toPipe()).complete();
    assertEquals(Arrays.asList("0", "115200000", "asdf"), getOutputStrings());
  }

  // note(duxbury): this doesn't verify anything. it's meant to be used for manually observing the results of the stack trace goodness.
  public void testFunctionStackTraces() {
    try {
      CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), Pump.prime().each(new FailingFunction()).toPipe()).complete();
      fail("was expecting a failure");
    } catch (Exception e) {
      // expecting an exception here
    }
  }

  // note(duxbury): this doesn't verify anything. it's meant to be used for manually observing the results of the stack trace goodness.
  public void testFilterStackTraces() {
    try {
      CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), Pump.prime().each(new FailingFilter()).toPipe()).complete();
      fail("was expecting a failure");
    } catch (Exception e) {
      // expecting an exception here
    }
  }

  public void testBranching() throws Exception {
    Pump common = Pump.prime()
        .retain("offset", "line");

    Pump branch1 = common.branch().groupby("offset")
        .every(new Count());

    Pump branch2 = common.branch().groupby("line")
        .every(new Count());

    FlowDef flowDef = new FlowDef()
        .addSource("input", getInTap())
        .addTailSink(branch1.toPipe(), getOutTap())
        .addTailSink(branch2.toPipe(), getOutTap2());

    // really just looking to see if this will plan and execute at all; results are meaningless
    CascadingHelper.get().getFlowConnector().connect(flowDef).complete();
  }

  public void testAggregateBy() throws Exception {
    Pipe p = Pump.prime()
        .groupby("line")
        .aggregateby(new MaxFunctor(), new Max(), "offset")
        .retain("max")
        .coerce("max", int.class)
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("10", "12", "22"), getOutputStrings());
  }

  public void testMultipleAggregateBy() throws Exception {
    Pipe p = Pump.prime()
        .groupby("line")
        .count("count")
        .sum("offset", "sum")
        .retain("line", "count", "sum")
        .coerce("sum", int.class)
        .toPipe();

    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("0\t1\t10", "115200000\t2\t12", "asdf\t1\t22"), getOutputStrings());
  }

  public void testMixAggregateBy() throws Exception {
    try {
      Pipe p = Pump.prime()
          .groupby("line")
          .every(new Sum(new Fields("sum"), int.class), "offset")
          .count("count")
          .toPipe();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // success!
    }
  }

  public void testSortedAggregateBy() throws Exception {
    try {
      Pipe p = Pump.prime()
          .groupby("line").secondarySort("offset")
          .first("offset")
          .toPipe();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // success!
    }
  }

  public void testGetSerializedClasses() {
    Pump pump = Pump.prime()
        .each(new FunctionThatKnows(Left.class));
    assertEquals(Collections.singleton(Left.class), pump.getEmittedClasses());

    pump = Pump.prime()
        .each(new FunctionThatKnows(Left.class))
        .each(new FilterNull());
    assertEquals(Collections.singleton(Left.class), pump.getEmittedClasses());

    pump = Pump.cogroup(
        Pump.prime().each(new FunctionThatKnows(Left.class)),
        Pump.prime().each(new FunctionThatKnows(Right.class)));
    assertEquals(new HashSet<Class>(Arrays.asList(Left.class,
        Right.class)), pump.getEmittedClasses());
  }

  private static class Left {}
  private static class Right {}

  private class FunctionThatKnows extends BaseOperation implements KnowsEmittedClasses {

    private final Class klass;

    public FunctionThatKnows(Class klass) {
      this.klass = klass;
    }

    @Override public Set<Class> getEmittedClasses() {
      return Collections.singleton(klass);
    }
    @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    }
  }

  private List<String> getOutputStrings() throws IOException {
    TupleEntryIterator iter = getOutTap().openForRead(new HadoopFlowProcess(), null);
    List<String> results = new ArrayList<String>();
    while (iter.hasNext()) {
      results.add(iter.next().getString(1));
    }
    return results;
  }
  private static class BufferFirst extends BaseOperation implements Buffer {

    private BufferFirst() {
      super(new Fields("date2"));
    }
    @Override public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
      Iterator<TupleEntry> argumentsIterator = bufferCall.getArgumentsIterator();
      bufferCall.getOutputCollector().add(argumentsIterator.next().getTuple());
    }

  }
  private static class FailingFunction extends BaseOperation implements Function {
    @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      throw new RuntimeException("intentional failure kthxbye");
    }

  }
  private static class FailingFilter extends BaseOperation implements Filter {
    @Override public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      throw new RuntimeException("intentional failure kthxbye");
    }

  }
  private static class MaxFunctor implements AggregateBy.Functor {

    @Override public Fields getDeclaredFields() {
      return new Fields("max");
    }

    @Override
    public Tuple aggregate(FlowProcess flowProcess, TupleEntry args, Tuple context) {
      if (context == null) {
        context = args.getTupleCopy();
      } else {
        context.set(0, Math.max(context.getDouble(0), args.getDouble(0)));
      }
      return context;
    }
    @Override public Tuple complete(FlowProcess flowProcess, Tuple context) {
      return context;
    }

  }
}
