package com.xgen.mongot.bootstrap;

import com.google.common.net.HostAndPort;
import com.xgen.mongot.community.MongotCommunity;
import com.xgen.mongot.standalone.MongotStandalone;
import com.xgen.mongot.standalone.config.CoexistenceConfig;
import picocli.CommandLine;

/** Selects standalone or combined ingestion from the configured MongoDB source. */
public final class Mongot {
  private Mongot() {}

  /** Starts the ingestion paths selected by the YAML source configuration or native CLI options. */
  public static void main(String[] args) {
    var community = new MongotCommunity();
    var command =
        new CommandLine(community).registerConverter(HostAndPort.class, HostAndPort::fromString);
    command.setExecutionStrategy(
        parsed -> {
          if (CommandLine.printHelpIfRequested(parsed)) {
            return 0;
          }
          try {
            boolean source =
                community.communityConfigCliOptionRegistry.traverse().keySet().stream()
                    .anyMatch(path -> path.startsWith("syncSource."));
            if (source || CoexistenceConfig.hasSyncSource(community.configPath)) {
              return new CommandLine.RunLast().execute(parsed);
            }
            return new CommandLine(new MongotStandalone()).execute(args);
          } catch (java.io.IOException error) {
            throw new CommandLine.ExecutionException(
                command, "Cannot read startup configuration", error);
          }
        });
    int result = command.execute(args);
    if (result != 0) {
      System.exit(result);
    }
  }
}
