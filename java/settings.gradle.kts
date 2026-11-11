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
// Mini StreamsForge host (sources → pipelines → tables, incl. partitioned tables) written once on actors-spi
// and run on all three runtimes.
include("host")
// Table-mode SQL compiled onto the differential dataflow library, differential-tested against engine's TableExecutor.
include("engine-dd")
