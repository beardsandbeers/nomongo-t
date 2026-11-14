package com.xgen.mongot.index.query.operators;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class JoinKeySetTest {
  @Test
  public void build_borrowedBinarySlice_preservesOwnedBytesAfterIteratorBufferChanges() {
    var reservations = new AtomicInteger();
    var builder = new JoinKeySet.Builder(reservations::addAndGet);
    byte[] buffer = {42, (byte) 0xff, (byte) 0x80, 43};
    builder.add(new BytesRef(buffer, 1, 2));
    builder.add(new BytesRef(new byte[] {(byte) 0xff, (byte) 0x80}));

    var keys = builder.build();
    buffer[1] = 0;

    assertThat(keys.size()).isEqualTo(1);
    assertThat(keys.contains(new BytesRef(new byte[] {(byte) 0xff, (byte) 0x80}))).isTrue();
    assertThat(keys.contains(new BytesRef(new byte[] {0, (byte) 0x80}))).isFalse();
    assertThat(reservations.get()).isEqualTo(2);
    assertThrows(IllegalStateException.class, () -> builder.add(new BytesRef("late")));
  }

  @Test
  public void add_reservationRejected_doesNotPublishUnreservedKey() {
    var builder =
        new JoinKeySet.Builder(
            ignored -> {
              throw new IllegalStateException("budget");
            });

    assertThrows(IllegalStateException.class, () -> builder.add(new BytesRef("key")));

    assertThat(builder.build().size()).isEqualTo(0);
  }
}
