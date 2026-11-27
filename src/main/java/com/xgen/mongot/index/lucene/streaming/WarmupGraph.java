package com.xgen.mongot.index.lucene.streaming;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.hnsw.HnswGraph;

/** Observes the native search so continuation can skip completed level-zero neighborhoods. */
final class WarmupGraph extends HnswGraph {
  private final HnswGraph graph;
  private final Set<Integer> completed = new HashSet<>();
  private int baseNode = -1;

  WarmupGraph(HnswGraph graph) {
    this.graph = graph;
  }

  Set<Integer> completedNodes(boolean visitLimitReached) {
    if (visitLimitReached) {
      // Lucene can read a whole adjacency list and truncate its final bulk scoring call.
      this.completed.remove(this.baseNode);
    }
    return this.completed;
  }

  @Override
  public void seek(int level, int target) throws IOException {
    this.graph.seek(level, target);
    this.baseNode = level == 0 ? target : -1;
  }

  @Override
  public int nextNeighbor() throws IOException {
    int neighbor = this.graph.nextNeighbor();
    if (neighbor == DocIdSetIterator.NO_MORE_DOCS && this.baseNode >= 0) {
      this.completed.add(this.baseNode);
    }
    return neighbor;
  }

  @Override
  public int size() {
    return this.graph.size();
  }

  @Override
  public int maxNodeId() {
    return this.graph.maxNodeId();
  }

  @Override
  public int neighborCount() {
    return this.graph.neighborCount();
  }

  @Override
  public int numLevels() throws IOException {
    return this.graph.numLevels();
  }

  @Override
  public int maxConn() {
    return this.graph.maxConn();
  }

  @Override
  public int entryNode() throws IOException {
    return this.graph.entryNode();
  }

  @Override
  public NodesIterator getNodesOnLevel(int level) throws IOException {
    return this.graph.getNodesOnLevel(level);
  }
}
