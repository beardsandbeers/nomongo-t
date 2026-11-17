package com.xgen.mongot.standalone.server;

import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexOwnership;
import com.xgen.mongot.standalone.grpc.ListIndexesRequest;
import com.xgen.mongot.standalone.grpc.ListIndexesResponse;
import com.xgen.mongot.standalone.runtime.DirectReadCatalog;
import io.grpc.Status;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

/** Bounded immutable metadata pages, never retained engine readers. */
final class DirectIndexPages {
  private static final long TTL = Duration.ofMinutes(2).toNanos();
  private static final long MAX_BYTES = 64L * 1024 * 1024;

  private record Page(
      String principal,
      IndexOwnership owner,
      int size,
      List<IndexDescription> remaining,
      long expires,
      long bytes) {}

  private final LinkedHashMap<String, Page> pages = new LinkedHashMap<>();
  private final DirectReadCatalog reads;
  private final int responseLimit;

  DirectIndexPages(DirectReadCatalog reads, int responseLimit) {
    this.reads = reads;
    this.responseLimit = responseLimit;
  }

  synchronized ListIndexesResponse list(ListIndexesRequest request) {
    long now = System.nanoTime();
    this.pages.values().removeIf(page -> now - page.expires() >= 0);
    if (request.getOwnership() == IndexOwnership.UNRECOGNIZED
        || request.getPageSize() < 0
        || request.getPageSize() > 500) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid ownership or page_size (maximum 500)")
          .asRuntimeException();
    }
    var principal = StandaloneAuthInterceptor.principal();
    List<IndexDescription> remaining;
    @com.google.errorprone.annotations.Var long expires = now + TTL;
    if (request.getPageToken().isEmpty()) {
      remaining =
          this.reads.snapshot().stream()
              .filter(index -> principal.allows(index.getIndexId()))
              .filter(
                  index ->
                      request.getOwnership() == IndexOwnership.INDEX_OWNERSHIP_UNSPECIFIED
                          || index.getOwnership() == request.getOwnership())
              .toList();
    } else {
      var page = this.pages.get(request.getPageToken());
      if (page == null
          || !page.principal().equals(principal.name())
          || page.owner() != request.getOwnership()
          || page.size() != request.getPageSize()) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Index page expired or parameters changed")
            .asRuntimeException();
      }
      var current =
          this.reads.snapshot().stream()
              .map(IndexDescription::getIndexId)
              .collect(java.util.stream.Collectors.toSet());
      if (page.remaining().stream()
          .anyMatch(
              index ->
                  !principal.allows(index.getIndexId()) || !current.contains(index.getIndexId()))) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Index visibility changed; restart listing")
            .asRuntimeException();
      }
      remaining = page.remaining();
      expires = page.expires();
    }
    var response = ListIndexesResponse.newBuilder();
    @com.google.errorprone.annotations.Var int count = 0;
    @com.google.errorprone.annotations.Var int bytes = 128;
    for (var index : remaining) {
      int cost = index.getSerializedSize() + 8;
      if ((request.getPageSize() > 0 && count >= request.getPageSize())
          || bytes + cost > this.responseLimit) {
        break;
      }
      response.addIndexes(index);
      bytes += cost;
      count++;
    }
    if (count < remaining.size()) {
      if (request.getPageSize() == 0 || count == 0) {
        throw Status.RESOURCE_EXHAUSTED
            .withDescription("Index listing exceeds byte limit; use pagination or GetIndex")
            .asRuntimeException();
      }
      var rest = List.copyOf(remaining.subList(count, remaining.size()));
      long weight = rest.stream().mapToLong(index -> index.getSerializedSize() + 256L).sum();
      if (weight > MAX_BYTES) {
        throw Status.RESOURCE_EXHAUSTED
            .withDescription("Index metadata snapshot exceeds cache budget")
            .asRuntimeException();
      }
      @com.google.errorprone.annotations.Var
      long retained = this.pages.values().stream().mapToLong(Page::bytes).sum();
      while (!this.pages.isEmpty()
          && (this.pages.size() >= 1024 || retained + weight > MAX_BYTES)) {
        var oldest = this.pages.entrySet().iterator();
        retained -= oldest.next().getValue().bytes();
        oldest.remove();
      }
      String token = UUID.randomUUID().toString();
      this.pages.put(
          token,
          new Page(
              principal.name(),
              request.getOwnership(),
              request.getPageSize(),
              rest,
              expires,
              weight));
      response.setNextPageToken(token);
    }
    return response.build();
  }
}
