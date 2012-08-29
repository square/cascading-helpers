cascading-helpers
=================

A whole bunch of functions, filters, and other tools that make writing Cascading flows a joy.

If you'd like to make your tests run faster, check out CascadingHelper. Example pseudocode:

    // put this line in your test's setUp() method
    CascadingHelper.setTestMode();
    // build your assembly
    ...
    // use CascadingHelper's getFlowConnector instead of instantiating one directly
    // and your tests will now run much faster!
    CascadingHelper.get().getFlowConnector().connect(...).complete();

If you'd like to make it easier to write your Flows and make them more readable, check out Pump:

    Pump.prime()
      .each(new RegexFilter("^[0-9]+"), "line")
      .each(new RegexSplitter(new Fields("timestamp", "tag"), ","), "line")
      .coerce(long.class, "timestamp")
      .each(new BucketizeTimestamp(), "bucketized_timestamp")
      .discard(timestamp)
      .rename("bucketized_timestamp", "timestamp");
      .groupby("timestamp", "tag");
      .every(new Count(new Fields("count")));

More to come!