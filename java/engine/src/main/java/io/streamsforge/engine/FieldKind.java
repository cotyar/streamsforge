package io.streamsforge.engine;

/** Json: the field may hold a nested JSON-like structure — {@code Map<String,Object>} and {@code List<Object>}
 *  nodes with primitive (String/Double/Long/Boolean/null) leaves. Accessed via the dialect's '->'/'->>' operators. */
public enum FieldKind { STRING, DOUBLE, LONG, BOOL, TIMESTAMP, JSON }
