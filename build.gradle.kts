import java.time.Duration

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    java
    `maven-publish`
    signing
    id("com.gradleup.shadow") version "9.3.1"
    id("com.diffplug.spotless") version "6.19.0"
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}

version = "0.1.0-SNAPSHOT"
group = "io.github.alvinhenrick"

description = "A Spark DataSource V2 for reading BSON files into DataFrames."

repositories {
    mavenCentral()
}

val scalaVersion = project.findProperty("scalaVersion") as String? ?: "2.12"
val sparkVersion = if (scalaVersion == "2.13") "4.0.2" else "3.5.8"

extra.apply {
    set("bsonVersion", "5.5.1")
    set("sparkVersion", sparkVersion)
    set("scalaVersion", scalaVersion)

    set("junitVersion", "5.13.4")
    set("assertjVersion", "3.26.3")
}

sourceSets {
    main {
        java {
            srcDirs(
                "src/main/java",
                "src/main/java_scala_${scalaVersion.replace(".", "")}"
            )
        }
    }
}

dependencies {
    implementation("org.mongodb:bson:${project.extra["bsonVersion"]}")

    compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")

    shadow("org.mongodb:bson:${project.extra["bsonVersion"]}")

    testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")

    testImplementation(platform("org.junit:junit-bom:${project.extra["junitVersion"]}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:${project.extra["assertjVersion"]}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(11)
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    jvmArgs(
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
    )
}

spotless {
    java {
        importOrder("java", "org", "com")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
        indentWithSpaces(4)
        palantirJavaFormat().style("GOOGLE")
    }
}

tasks.shadowJar {
    configurations = listOf(project.configurations.shadow.get())
    archiveClassifier.set("")
    archiveBaseName.set("bson-spark_$scalaVersion")
}

tasks.register<Jar>("sourcesJar") {
    from(sourceSets.main.get().allSource)
    archiveClassifier.set("sources")
}

tasks.register<Jar>("javadocJar") {
    from(tasks.javadoc)
    archiveClassifier.set("javadoc")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "bson-spark_$scalaVersion"
            from(components["java"])
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])

            pom {
                name.set("bson-spark")
                description.set(project.description)
                url.set("https://github.com/alvinhenrick/bson-spark")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("alvinhenrick")
                        name.set("Alvin Henrick")
                        url.set("https://github.com/alvinhenrick")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/alvinhenrick/bson-spark.git")
                    developerConnection.set("scm:git:ssh://github.com:alvinhenrick/bson-spark.git")
                    url.set("https://github.com/alvinhenrick/bson-spark")
                }
            }
        }
    }
}

nexusPublishing {
    repositories {
        sonatype {
            val nexusUsername: String? by project
            val nexusPassword: String? by project
            username.set(nexusUsername ?: "")
            password.set(nexusPassword ?: "")
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(
                uri("https://central.sonatype.com/repository/maven-snapshots/")
            )
        }
    }
    connectTimeout.set(Duration.ofMinutes(5))
    clientTimeout.set(Duration.ofMinutes(30))
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["mavenJava"])
}
