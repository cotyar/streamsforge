package io.streamsforge.host;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** A scenario test that runs unmodified on futures, loom and pekko; the one parameter is the {@link Fx.Rt}. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest(name = "{0}")
@MethodSource("io.streamsforge.host.Fx#runtimes")
@interface OnAllRuntimes {}
