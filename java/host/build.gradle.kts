dependencies {
    api(project(":engine"))
    api(project(":actors-spi"))
    testImplementation(project(":actors-futures"))
    testImplementation(project(":actors-loom"))
    testImplementation(project(":actors-pekko"))
}

// -Dbench=true enables the (otherwise skipped) HostBenchmarkTest.
tasks.test {
    systemProperty("bench", providers.systemProperty("bench").orElse("false").get())
}
