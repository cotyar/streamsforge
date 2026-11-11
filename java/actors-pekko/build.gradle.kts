dependencies {
    api(project(":actors-spi"))
    implementation(libs.pekko.actor.typed)
    testImplementation(testFixtures(project(":actors-spi")))
    testImplementation(libs.pekko.actor.testkit.typed)
}
