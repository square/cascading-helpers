package org.ch.pump;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.DateFormatter;
import cascading.pipe.Pipe;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ch.CascadingHelper;

/** Author: duxbury */
public class TestPump extends TestCase {

  private static final String INPUT_PATH = "/tmp/TestPump/input";
  private static final String INPUT2_PATH = "/tmp/TestPump/input2";
  private static final String OUTPUT_PATH = "/tmp/TestPump/output";

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

    Pipe nsp = left.cogroup(right, "date").toPipe();
    Pipe stp = Pump.cogroup(left, right, "date").toPipe();
    assertEquals(nsp.toString(), stp.toString());
    assertTrue(Arrays.equals(nsp.getHeads(), stp.getHeads()));
  }
  
  public void testUnique() throws Exception {
    CascadingHelper.get().getFlowConnector().connect(getInTap(), getOutTap(), Pump.prime().retain("line").unique("line").toPipe()).complete();
    assertEquals(Arrays.asList("0", "115200000", "asdf"), getOutputStrings());
  }

  private List<String> getOutputStrings() throws IOException {
    TupleEntryIterator iter = getOutTap().openForRead(new HadoopFlowProcess(), null);
    List<String> results = new ArrayList<String>();
    while (iter.hasNext()) {
      results.add(iter.next().getString(1));
    }
    return results;
  }
}
