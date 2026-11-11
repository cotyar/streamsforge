rootProject.name = "streamsforge-java"

// Differential dataflow library (no dependencies, usable on its own).
include("dataflow")
// Port of shared/StreamsForge.Engine: SQL tokenizer/parser/validator/planner + pipeline/table executors.
include("engine")
// Runtime-agnostic, turn-based virtual-actor SPI (the Orleans-grain shape) ...
include("actors-spi")
// ... and its three implementations.
include("actors-futures")   // CompletableFuture mailboxes
include("actors-loom")      // one virtual thread (JDK 21 "green thread") per activation
include("actors-pekko")     // Apache Pekko typed actors
