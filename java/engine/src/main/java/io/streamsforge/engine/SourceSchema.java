package io.streamsforge.engine;

import java.util.Map;

/** Schema of a source stream, as the validator sees it. {@code fields} keeps insertion order. */
public record SourceSchema(String name, Map<String, FieldKind> fields) {}
