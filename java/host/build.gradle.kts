dependencies {
    api(project(":engine"))
    api(project(":actors-spi"))
    testImplementation(project(":actors-futures"))
    testImplementation(project(":actors-loom"))
    testImplementation(project(":actors-pekko"))
}
