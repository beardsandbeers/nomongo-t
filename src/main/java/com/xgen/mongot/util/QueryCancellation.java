package com.xgen.mongot.util;

/** Transport-independent cancellation for counting and reduction work outside Lucene search. */
public final class QueryCancellation {
  private static final Runnable NOOP = QueryCancellation::noOp;
  private static final ThreadLocal<Runnable> CURRENT = ThreadLocal.withInitial(() -> NOOP);

  private QueryCancellation() {}

  private static void noOp() {}

  /** Capture at construction when work may later run on a partition/counting worker. */
  public static Runnable capture() {
    return CURRENT.get();
  }

  public static void check() {
    CURRENT.get().run();
  }

  public static Scope openScope(Runnable check) {
    var previous = CURRENT.get();
    CURRENT.set(check);
    return new Scope(previous);
  }

  public static final class Scope implements AutoCloseable {
    private final Runnable previous;

    private Scope(Runnable previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      if (this.previous == NOOP) {
        CURRENT.remove();
      } else {
        CURRENT.set(this.previous);
      }
    }
  }
}
