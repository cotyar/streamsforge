package io.streamsforge.engine.sql;

/** Which namespace/rule set the {@link Validator} runs in (windowed pipelines vs persistent tables). */
public enum ValidationMode { STREAM, TABLE }
