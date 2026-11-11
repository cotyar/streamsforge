package io.streamsforge.engine;

import static org.junit.jupiter.api.Assertions.*;

import io.streamsforge.engine.runtime.FieldValueConversion;
import io.streamsforge.engine.runtime.FieldValueConversion.Coercion;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Plan 009 Round C wave C1 — direct unit coverage of {@link FieldValueConversion}, the public, {@link FieldKind}-keyed
 * canonical conversion behind the SQL dialect's TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/TO_STRING functions.
 *
 * <p>CLR-to-Java value mapping used by this port (see {@code FieldValueConversion}'s class Javadoc): {@code DateTime}
 * Kind=Utc -&gt; {@link Instant}; Kind=Unspecified -&gt; {@link LocalDateTime} (read as UTC); Kind=Local -&gt;
 * {@link java.time.ZonedDateTime} in the system zone; {@code DateTimeOffset} -&gt; {@link OffsetDateTime};
 * {@code decimal} -&gt; {@link BigDecimal}; {@code short}/{@code ushort} -&gt; Short/Integer; {@code byte}/{@code sbyte}
 * -&gt; Short/Byte; {@code uint} -&gt; Long; {@code ulong} -&gt; {@link BigInteger}.
 */
class FieldValueConversionTests {
    private static Coercion coerce(FieldKind kind, Object input) { return FieldValueConversion.tryCoerce(kind, input); }

    private static Map<String, Object> dict(Object... kv) {
        var m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // 2026-08-15T12:00:00Z
    private static final Instant UTC = Instant.parse("2026-08-15T12:00:00Z");
    private static final long EXPECTED_MS = 1_786_795_200_000L;

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.String, …) — always succeeds
    // ------------------------------------------------------------------

    @Test
    void string_passes_a_string_through_unchanged() {
        var c = coerce(FieldKind.STRING, "hello");
        assertTrue(c.ok());
        assertEquals("hello", c.value());
    }

    static Stream<Arguments> numberToStringCases() {
        return Stream.of(Arguments.of(42L, "42"), Arguments.of(3.5, "3.5"));
    }

    @ParameterizedTest
    @MethodSource("numberToStringCases")
    void string_coerces_a_number_via_invariant_ToString(Object input, String expected) {
        var c = coerce(FieldKind.STRING, input);
        assertTrue(c.ok());
        assertEquals(expected, c.value());
    }

    @ParameterizedTest
    @CsvSource({"true,true", "false,false"})
    void string_coerces_a_bool_as_lowercase_literal(boolean input, String expected) {
        var c = coerce(FieldKind.STRING, input);
        assertTrue(c.ok());
        assertEquals(expected, c.value());
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Double, …)
    // ------------------------------------------------------------------

    /** A date/time reaching a String field renders as one ISO-8601 UTC shape, identical to what
     *  TO_STRING(TO_TIMESTAMP(x)) emits. */
    @Test
    void string_renders_a_date_time_as_ISO_8601_UTC() {
        final String iso = "2026-08-15T12:00:00.000Z";

        var fromUtc = coerce(FieldKind.STRING, UTC);
        assertTrue(fromUtc.ok());
        assertEquals(iso, fromUtc.value());

        var fromUnspec = coerce(FieldKind.STRING, LocalDateTime.of(2026, 8, 15, 12, 0, 0));
        assertTrue(fromUnspec.ok());
        assertEquals(iso, fromUnspec.value());

        // An offset-bearing value normalises to UTC rather than printing its offset.
        var fromOffset = coerce(FieldKind.STRING, OffsetDateTime.of(2026, 8, 15, 14, 0, 0, 0, ZoneOffset.ofHours(2)));
        assertTrue(fromOffset.ok());
        assertEquals(iso, fromOffset.value());

        // Round-trips: the text this produces is text TryToTimestamp reads back to the same instant.
        var backAgain = FieldValueConversion.tryToTimestamp(iso);
        assertTrue(backAgain.ok());
        assertEquals(UTC.toEpochMilli(), backAgain.value());
    }

    /** A date/time column declared as a Timestamp field: Utc and Local carry their own offset and convert exactly;
     *  Unspecified is read as UTC. */
    @Test
    void timestamp_accepts_clr_date_times_on_every_DateTimeKind() {
        long expected = EXPECTED_MS;
        assertEquals(expected, UTC.toEpochMilli()); // pins the constant itself

        var fromUtc = coerce(FieldKind.TIMESTAMP, UTC);
        assertTrue(fromUtc.ok());
        assertEquals(expected, fromUtc.value());

        // Unspecified — a Postgres `timestamp without time zone` — reads as the same instant, NOT as
        // whatever the host process's timezone would make of it.
        var fromUnspecified = coerce(FieldKind.TIMESTAMP, LocalDateTime.of(2026, 8, 15, 12, 0, 0));
        assertTrue(fromUnspecified.ok());
        assertEquals(expected, fromUnspecified.value());

        // Local carries a known offset, so it converts to the instant it actually denotes.
        var local = UTC.atZone(ZoneId.systemDefault());
        var fromLocal = coerce(FieldKind.TIMESTAMP, local);
        assertTrue(fromLocal.ok());
        assertEquals(expected, fromLocal.value());

        var fromOffset = coerce(FieldKind.TIMESTAMP, UTC.atOffset(ZoneOffset.UTC));
        assertTrue(fromOffset.ok());
        assertEquals(expected, fromOffset.value());

        // An offset that is NOT zero must be honoured, not dropped.
        var plusTwo = OffsetDateTime.of(2026, 8, 15, 14, 0, 0, 0, ZoneOffset.ofHours(2));
        var fromPlusTwo = coerce(FieldKind.TIMESTAMP, plusTwo);
        assertTrue(fromPlusTwo.ok());
        assertEquals(expected, fromPlusTwo.value());
    }

    /** The same three rules, reached through the two other timestamp entry points, so they cannot drift apart. */
    @Test
    void the_other_two_timestamp_entry_points_agree_with_TryCoerce() {
        long expected = EXPECTED_MS;
        var unspecified = LocalDateTime.of(2026, 8, 15, 12, 0, 0);

        var fnUtc = FieldValueConversion.tryToTimestamp(UTC);
        assertTrue(fnUtc.ok());
        assertEquals(expected, fnUtc.value());
        var fnOffset = FieldValueConversion.tryToTimestamp(UTC.atOffset(ZoneOffset.UTC));
        assertTrue(fnOffset.ok());
        assertEquals(expected, fnOffset.value());
        var fnUnspec = FieldValueConversion.tryToTimestamp(unspecified);
        assertTrue(fnUnspec.ok());
        assertEquals(expected, fnUnspec.value());

        assertEquals(expected, FieldValueConversion.resolveTimestamp(UTC, 1));
        assertEquals(expected, FieldValueConversion.resolveTimestamp(UTC.atOffset(ZoneOffset.UTC), 1));
        assertEquals(expected, FieldValueConversion.resolveTimestamp(unspecified, 1));
    }

    /** A date/time is meaningful for Timestamp only. Long stays "a number". */
    @Test
    void long_does_not_silently_accept_a_date_time() {
        var c = coerce(FieldKind.LONG, UTC);
        assertFalse(c.ok());
        assertNull(c.value());
    }

    static Stream<Object> driverNumerics() {
        // 12.34m, (short)7, (ushort)7, (byte)7, (sbyte)7, 7u, 7ul
        return Stream.of(new BigDecimal("12.34"), (short) 7, 7 /* ushort */, (short) 7 /* byte */, (byte) 7 /* sbyte */, 7L /* uint */, BigInteger.valueOf(7) /* ulong */);
    }

    /** The OTC-demo finding: a Postgres {@code numeric} column arrives as CLR {@code decimal}, and before this arm
     *  every money column declared Double coerced to NULL. */
    @ParameterizedTest
    @EnumSource(value = FieldKind.class, names = {"DOUBLE", "LONG"})
    void driver_numerics_beyond_long_and_double_coerce_rather_than_null(FieldKind kind) {
        var inputs = driverNumerics().toList();
        for (var input : inputs) {
            var c = coerce(kind, input);
            assertTrue(c.ok(), input.getClass().getSimpleName() + " failed to coerce to " + kind);
            assertNotNull(c.value());
        }
    }

    @Test
    void decimal_coerces_to_double_by_value_and_to_long_by_truncation() {
        var asDouble = coerce(FieldKind.DOUBLE, new BigDecimal("12.34"));
        assertTrue(asDouble.ok());
        assertEquals(Double.class, asDouble.value().getClass());
        assertEquals(12.34d, (double) asDouble.value(), 1e-10);

        var asLong = coerce(FieldKind.LONG, new BigDecimal("12.9"));
        assertTrue(asLong.ok());
        assertEquals(12L, asLong.value()); // toward zero, matching the existing double-to-long arm

        var zero = coerce(FieldKind.BOOL, new BigDecimal("0"));
        assertTrue(zero.ok());
        assertEquals(false, zero.value());
        var nonZero = coerce(FieldKind.BOOL, new BigDecimal("0.5"));
        assertTrue(nonZero.ok());
        assertEquals(true, nonZero.value());
    }

    static Stream<Object> doubleAcceptedInputs() {
        return Stream.of(3.5, 3L, true);
    }

    @ParameterizedTest
    @MethodSource("doubleAcceptedInputs")
    void double_coercion_accepts_numeric_and_bool_types(Object input) {
        var c = coerce(FieldKind.DOUBLE, input);
        assertTrue(c.ok());
        assertEquals(Double.class, c.value().getClass());
    }

    @Test
    void double_coercion_accepts_a_numeric_string() {
        var c = coerce(FieldKind.DOUBLE, "3.5");
        assertTrue(c.ok());
        assertEquals(3.5, c.value());
    }

    @Test
    void double_coercion_rejects_a_non_numeric_string() {
        var c = coerce(FieldKind.DOUBLE, "not-a-number");
        assertFalse(c.ok());
        assertNull(c.value());
    }

    @Test
    void double_coercion_rejects_an_empty_string() {
        var c = coerce(FieldKind.DOUBLE, "");
        assertFalse(c.ok());
        assertNull(c.value());
    }

    @Test
    void double_coercion_rejects_an_unsupported_type() {
        var c = coerce(FieldKind.DOUBLE, new Object());
        assertFalse(c.ok());
        assertNull(c.value());
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Long / .Timestamp, …) — deliberately identical rule (see class doc)
    // ------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = FieldKind.class, names = {"LONG", "TIMESTAMP"})
    void long_and_Timestamp_share_identical_coercion(FieldKind kind) {
        var fromString = coerce(kind, "42");
        assertTrue(fromString.ok());
        assertEquals(42L, fromString.value());

        var fromDouble = coerce(kind, 3.9);
        assertTrue(fromDouble.ok());
        assertEquals(3L, fromDouble.value()); // truncates, does not round

        var fromBool = coerce(kind, true);
        assertTrue(fromBool.ok());
        assertEquals(1L, fromBool.value());

        assertFalse(coerce(kind, "not-a-number").ok());
        assertFalse(coerce(kind, new Object()).ok());
    }

    @Test
    void long_coercion_rejects_an_overflowing_numeric_string() {
        // long.TryParse fails outright for a string outside the long range — the one overflow path
        // that DOES come back NULL.
        var c = coerce(FieldKind.LONG, "99999999999999999999");
        assertFalse(c.ok());
        assertNull(c.value());
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Bool, …) — pinned to FieldValueCoercion.TryToBool's existing, permissive rule
    // ------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"true,true", "TRUE,true", "false,false", "FALSE,false", "'',false", "0,false", "anything-else,true"}) // last: documented Finding
    void bool_coercion_from_string(String input, boolean expected) {
        var c = coerce(FieldKind.BOOL, input);
        assertTrue(c.ok());
        assertEquals(expected, c.value());
    }

    static Stream<Arguments> boolFromNumberCases() {
        return Stream.of(
            Arguments.of(1L, true),
            Arguments.of(0L, false),
            Arguments.of(2.5, true), // nonzero-is-true, not "must equal 1" — matches FieldValueCoercion
            Arguments.of(0.0, false));
    }

    @ParameterizedTest
    @MethodSource("boolFromNumberCases")
    void bool_coercion_from_number(Object input, boolean expected) {
        var c = coerce(FieldKind.BOOL, input);
        assertTrue(c.ok());
        assertEquals(expected, c.value());
    }

    @Test
    void bool_coercion_rejects_an_unsupported_type() {
        assertFalse(coerce(FieldKind.BOOL, new Object()).ok());
    }

    // ------------------------------------------------------------------
    // TryCoerce(FieldKind.Json, …) — structural passthrough, never fails
    // ------------------------------------------------------------------

    @Test
    void json_coercion_always_passes_the_value_through_unchanged() {
        var dict = dict("a", 1L);
        var c = coerce(FieldKind.JSON, dict);
        assertTrue(c.ok());
        assertSame(dict, c.value());
    }

    // ------------------------------------------------------------------
    // TryToTimestamp — the TO_TIMESTAMP SQL function's own, wider rule
    // ------------------------------------------------------------------

    @Test
    void tryToTimestamp_accepts_epoch_ms_as_a_number() {
        var c = FieldValueConversion.tryToTimestamp(1_700_000_000_000L);
        assertTrue(c.ok());
        assertEquals(1_700_000_000_000L, c.value());
    }

    @Test
    void tryToTimestamp_accepts_epoch_ms_as_a_numeric_string() {
        // The genuine superset over both TryCoerce(Timestamp, …) (no ISO text) and ResolveTimestamp
        // (no numeric-string) — see class doc.
        var c = FieldValueConversion.tryToTimestamp("1700000000000");
        assertTrue(c.ok());
        assertEquals(1_700_000_000_000L, c.value());
    }

    @Test
    void tryToTimestamp_accepts_iso8601_text() {
        var c = FieldValueConversion.tryToTimestamp("2023-11-14T22:13:20Z");
        assertTrue(c.ok());
        assertEquals(1_700_000_000_000L, c.value());
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-timestamp", ""})
    void tryToTimestamp_rejects_unparseable_text(String input) {
        var c = FieldValueConversion.tryToTimestamp(input);
        assertFalse(c.ok());
        assertNull(c.value());
    }

    @Test
    void tryToTimestamp_rejects_bool_and_unsupported_types() {
        assertFalse(FieldValueConversion.tryToTimestamp(true).ok());
        assertFalse(FieldValueConversion.tryToTimestamp(new Object()).ok());
    }

    // ------------------------------------------------------------------
    // ResolveTimestamp — exact port of AppCore's RowTimestamp.Resolve (fallback-shaped, not NULL-shaped)
    // ------------------------------------------------------------------

    @Test
    void resolveTimestamp_returns_a_long_value_as_is() {
        assertEquals(1234L, FieldValueConversion.resolveTimestamp(1234L, 999));
    }

    @Test
    void resolveTimestamp_truncates_a_double() {
        assertEquals(3L, FieldValueConversion.resolveTimestamp(3.9, 999));
    }

    @Test
    void resolveTimestamp_parses_iso8601_text() {
        assertEquals(1_700_000_000_000L, FieldValueConversion.resolveTimestamp("2023-11-14T22:13:20Z", 999));
    }

    @Test
    void resolveTimestamp_falls_back_on_a_numeric_string_unlike_TryToTimestamp() {
        // The one deliberate behavioral difference from TryToTimestamp: ResolveTimestamp has no
        // numeric-string support (matching RowTimestamp.Resolve exactly), so a numeric string falls
        // back rather than parsing as epoch-ms.
        assertEquals(999L, FieldValueConversion.resolveTimestamp("1700000000000", 999));
    }

    @Test
    void resolveTimestamp_falls_back_on_unparseable_or_missing_input() {
        assertEquals(999L, FieldValueConversion.resolveTimestamp("garbage", 999));
        assertEquals(999L, FieldValueConversion.resolveTimestamp(null, 999));
    }
}
