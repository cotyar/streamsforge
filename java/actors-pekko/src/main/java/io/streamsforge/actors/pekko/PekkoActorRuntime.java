package io.streamsforge.actors.pekko;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.streamsforge.actors.*;
import io.streamsforge.actors.pekko.Protocol.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

/**
 * Virtual actors on Apache Pekko typed actors.
 *
 * <p>Two tiers (see {@link Shard} and {@link Entity}): a fixed set of <em>shard</em> actors partition
 * the key space and own placement and passivation; each activation is an <em>entity</em> actor spawned
 * as a child of its shard. A message goes caller → shard → entity. The extra hop is the price of an
 * airtight deactivation protocol (the one Pekko Cluster Sharding uses) — see {@link Shard}.
 */
public final class PekkoActorRuntime implements ActorRuntime {

    private static final AtomicInteger SYSTEMS = new AtomicInteger();

    private final ActorSystem<Void> system;
    private final List<org.apache.pekko.actor.typed.ActorRef<ShardMsg>> shards;
    private final ConcurrentHashMap<String, ActorType<Object>> types = new ConcurrentHashMap<>();
    private final Set<ActorKey> active = ConcurrentHashMap.newKeySet();
    private volatile Consumer<Throwable> errorHandler = t -> t.printStackTrace();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeDone = new CompletableFuture<>();
    private volatile boolean closed;

    public PekkoActorRuntime() {
        this(defaultConfig());
    }

    /** @param config merged over {@code pekko.*} defaults; use to tune dispatchers. */
    public PekkoActorRuntime(Config config) {
        int n = Math.max(4, 2 * Runtime.getRuntime().availableProcessors());
        CompletableFuture<List<org.apache.pekko.actor.typed.ActorRef<ShardMsg>>> refs = new CompletableFuture<>();
        this.system = ActorSystem.create(
                Behaviors.<Void>setup(ctx -> {
                    List<org.apache.pekko.actor.typed.ActorRef<ShardMsg>> list = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) list.add(ctx.spawn(Shard.create(this), "shard-" + i));
                    refs.complete(list);
                    return Behaviors.empty();
                }),
                "actors-" + SYSTEMS.incrementAndGet(),
                config);
        this.shards = refs.join();
    }

    static Config defaultConfig() {
        return ConfigFactory.parseString(
                        """
                        pekko.loglevel = "WARNING"
                        pekko.log-dead-letters = off
                        pekko.log-dead-letters-during-shutdown = off
                        pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
                        """)
                .withFallback(ConfigFactory.load());
    }

    @Override
    public String name() {
        return "pekko";
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S> void register(ActorType<S> type) {
        if (types.putIfAbsent(type.name(), (ActorType<Object>) type) != null) {
            throw new IllegalArgumentException("actor type already registered: " + type.name());
        }
    }

    ActorType<Object> typeOf(ActorKey key) {
        ActorType<Object> t = types.get(key.type());
        if (t == null) throw new IllegalArgumentException("unknown actor type: " + key.type());
        return t;
    }

    Set<ActorKey> activeSet() {
        return active;
    }

    @Override
    public <S> ActorRef<S> ref(ActorKey key) {
        typeOf(key);
        return new Ref<>(key);
    }

    @Override
    public Set<ActorKey> activeKeys() {
        return Set.copyOf(active);
    }

    @Override
    public void onUnhandledError(Consumer<Throwable> handler) {
        this.errorHandler = handler;
    }

    void report(Throwable t) {
        try {
            errorHandler.accept(t);
        } catch (Throwable ignored) {
            // the error sink must not take the runtime down
        }
    }

    private org.apache.pekko.actor.typed.ActorRef<ShardMsg> shardOf(ActorKey key) {
        return shards.get((key.hashCode() & 0x7fffffff) % shards.size());
    }

    private void send(ActorKey key, Work w) {
        if (closed) {
            w.fail(new IllegalStateException("runtime closed"));
            return;
        }
        shardOf(key).tell(new Deliver(key, w));
    }

    @Override
    public CompletionStage<Void> deactivate(ActorKey key) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (closed) {
            done.complete(null);
            return done;
        }
        shardOf(key).tell(new DeactivateKey(key, done));
        return done;
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            closeDone.join();
            return;
        }
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            do {
                List<CompletableFuture<Void>> fs = new ArrayList<>();
                for (var shard : shards) {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    shard.tell(new DeactivateAll(f));
                    fs.add(f);
                }
                for (CompletableFuture<Void> f : fs) {
                    try {
                        f.get(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        report(e);
                    }
                }
            } while (!active.isEmpty() && System.nanoTime() < deadline);
        } finally {
            closed = true;
            system.terminate();
            try {
                system.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                report(e);
            }
            closeDone.complete(null);
        }
    }

    // ------------------------------------------------------------------------------------ refs / work

    private final class Ref<S> implements ActorRef<S> {
        private final ActorKey key;

        Ref(ActorKey key) {
            this.key = key;
        }

        @Override
        public ActorKey key() {
            return key;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> CompletionStage<R> ask(Turn<S, R> turn) {
            CompletableFuture<R> f = new CompletableFuture<>();
            send(key, new AskWork((Turn<Object, Object>) turn, (CompletableFuture<Object>) f));
            return f;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void tell(Consumer<S> fn) {
            send(key, new TellWork((Consumer<Object>) fn));
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Ref<?> r && r.key.equals(key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public String toString() {
            return "ref(" + key + ")";
        }
    }

    private static final class AskWork implements Work {
        private final Turn<Object, Object> turn;
        private final CompletableFuture<Object> result;

        AskWork(Turn<Object, Object> turn, CompletableFuture<Object> result) {
            this.turn = turn;
            this.result = result;
        }

        @Override
        public CompletionStage<?> run(ActorContext<Object> ctx) throws Exception {
            return turn.run(ctx);
        }

        @Override
        public void complete(Object value) {
            result.complete(value);
        }

        @Override
        public void fail(Throwable t) {
            result.completeExceptionally(t);
        }
    }

    private final class TellWork implements Work {
        private final Consumer<Object> fn;

        TellWork(Consumer<Object> fn) {
            this.fn = fn;
        }

        @Override
        public CompletionStage<?> run(ActorContext<Object> ctx) {
            fn.accept(ctx.state());
            return Turns.done();
        }

        @Override
        public void complete(Object value) {}

        @Override
        public void fail(Throwable t) {
            report(t);
        }
    }
}
