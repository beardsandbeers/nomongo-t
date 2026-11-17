package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.standalone.grpc.OperationsServiceGrpc;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.List;
import org.junit.Test;

public class StandaloneRpcDeadlineTest {
  private static final Metadata.Key<String> TIMEOUT =
      Metadata.Key.of("grpc-timeout", Metadata.ASCII_STRING_MARSHALLER);
  private static final String STATUS =
      OperationsServiceGrpc.getGetStatusMethod().getFullMethodName();
  private final StandaloneRpcDeadline tracer =
      new StandaloneRpcDeadline(
          List.of(new OperationsServiceGrpc.OperationsServiceImplBase() {}.bindService()),
          Duration.ofSeconds(1));

  @Test
  public void newServerStreamTracer_absentOrLongerTimeout_appliesServerMaximum() {
    var absent = new Metadata();
    var longer = new Metadata();
    longer.put(TIMEOUT, "99999999H");

    this.tracer.newServerStreamTracer(STATUS, absent);
    this.tracer.newServerStreamTracer(STATUS, longer);

    assertThat(absent.get(TIMEOUT)).isEqualTo("1000000u");
    assertThat(longer.getAll(TIMEOUT)).containsExactly("1000000u");
  }

  @Test
  public void newServerStreamTracer_clientTimeout_preservesEarlierExpiryAndParseErrors() {
    List.of("0n", "10m", "bad")
        .forEach(
            timeout -> {
              var headers = new Metadata();
              headers.put(TIMEOUT, timeout);

              this.tracer.newServerStreamTracer(STATUS, headers);

              assertThat(headers.getAll(TIMEOUT)).containsExactly(timeout);
            });
  }

  @Test
  public void newServerStreamTracer_nativeHealth_leavesTransportTimeoutAlone() {
    var headers = new Metadata();

    this.tracer.newServerStreamTracer("grpc.health.v1.Health/Check", headers);

    assertThat(headers.containsKey(TIMEOUT)).isFalse();
  }
}
