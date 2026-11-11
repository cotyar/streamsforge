package io.streamsforge.actors;

import java.util.Objects;

/**
 * Identity of a virtual actor: its type (the "grain class") and its id within that type.
 * Like an Orleans grain id, a key always names the same logical actor; whether it is currently
 * ACTIVE in memory is the runtime's business, not the caller's.
 */
public record ActorKey(String type, String id) {
    public ActorKey {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(id, "id");
    }

    @Override
    public String toString() {
        return type + "/" + id;
    }
}
