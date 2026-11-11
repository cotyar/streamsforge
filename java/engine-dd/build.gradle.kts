dependencies {
    api(project(":engine"))
    api(project(":dataflow"))
}

tasks.test {
    // Forward -Ddd.* knobs (case counts, bench sizes) from the Gradle invocation to the test JVM.
    // `-Djunit.jupiter.conditions.deactivate=org.junit.*DisabledCondition` runs the @Disabled bug repros (they must FAIL).
    for ((k, v) in System.getProperties()) {
        if (k.toString().startsWith("dd.") || k.toString().startsWith("junit.")) systemProperty(k.toString(), v.toString())
    }
    testLogging { showStandardStreams = true }
}
