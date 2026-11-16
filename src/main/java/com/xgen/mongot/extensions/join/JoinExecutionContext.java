package com.xgen.mongot.extensions.join;

import com.xgen.mongot.util.QueryCancellation;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Request-scoped cancellation, captured on dispatch and carried to worker threads. */
public final class JoinExecutionContext {
  private record Invocation(Runnable check, LongSupplier remainingNanos) {}

  private static final ThreadLocal<Invocation> CURRENT =
      ThreadLocal.withInitial(() -> new Invocation(() -> {}, () -> Long.MAX_VALUE));

  private JoinExecutionContext() {}

  public static Runnable capture() {
    return CURRENT.get().check();
  }

  public static long remainingNanos() {
    return CURRENT.get().remainingNanos().getAsLong();
  }

  public static <T> T run(Runnable check, Supplier<T> work) {
    return run(check, () -> Long.MAX_VALUE, work);
  }

  public static <T> T run(Runnable check, LongSupplier remainingNanos, Supplier<T> work) {
    var previous = CURRENT.get();
    CURRENT.set(new Invocation(check, remainingNanos));
    try (var cancellation = QueryCancellation.openScope(check)) {
      return work.get();
    } finally {
      CURRENT.set(previous);
    }
  }
}
