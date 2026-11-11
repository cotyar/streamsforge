dependencies {
    api(project(":actors-spi"))
    testImplementation(testFixtures(project(":actors-spi")))
}

// -Dbench=true enables the (otherwise skipped) *BenchmarkTest.
tasks.test {
    systemProperty("bench", providers.systemProperty("bench").orElse("false").get())
}
