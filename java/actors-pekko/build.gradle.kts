dependencies {
    api(project(":actors-spi"))
    implementation(libs.pekko.actor.typed)
    testImplementation(testFixtures(project(":actors-spi")))
    testImplementation(libs.pekko.actor.testkit.typed)
}

// -Dbench=true enables the (otherwise skipped) *BenchmarkTest.
tasks.test {
    systemProperty("bench", providers.systemProperty("bench").orElse("false").get())
}
