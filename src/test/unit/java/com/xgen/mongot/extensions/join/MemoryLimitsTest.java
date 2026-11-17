package com.xgen.mongot.extensions.join;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.extensions.join.JoinPlanner.MemoryLimits;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class MemoryLimitsTest {
  public record HeapBudget(long heap, long concurrent, long query) {}

  @DataPoints
  public static final HeapBudget[] HEAPS = {
    new HeapBudget(1, 1, 1),
    new HeapBudget(1L << 30, 128L << 20, 16L << 20),
    new HeapBudget(2L << 30, 256L << 20, 32L << 20),
    new HeapBudget(16L << 30, 2L << 30, 256L << 20),
    new HeapBudget(Long.MAX_VALUE, 1152921504606846975L, 144115188075855871L)
  };

  @DataPoints public static final long[] INVALID_HEAPS = {0, -1};

  @Theory
  public void fromHeapBytes_differentHeapSizes_scalesBothLimitsWithoutOverflow(HeapBudget budget) {
    var limits = MemoryLimits.fromHeapBytes(budget.heap());

    assertThat(limits.maxConcurrentBytes()).isEqualTo(budget.concurrent());
    assertThat(limits.maxQueryBytes()).isEqualTo(budget.query());
  }

  @Theory
  public void fromHeapBytes_nonPositiveHeap_rejects(long bytes) {
    assertThrows(IllegalArgumentException.class, () -> MemoryLimits.fromHeapBytes(bytes));
  }

  @Test
  public void fromConcurrentBytes_explicitPool_derivesQueryAllowance() {
    var limits = MemoryLimits.fromConcurrentBytes(512L << 20);

    assertThat(limits.maxConcurrentBytes()).isEqualTo(512L << 20);
    assertThat(limits.maxQueryBytes()).isEqualTo(64L << 20);
  }
}
