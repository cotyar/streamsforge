plugins { `java-test-fixtures` }

dependencies {
    testFixturesApi(platform("org.junit:junit-bom:5.11.4"))
    testFixturesApi("org.junit.jupiter:junit-jupiter")
}
