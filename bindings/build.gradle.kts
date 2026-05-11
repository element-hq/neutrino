import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.android)
    id("maven-publish")
}

repositories {
    google()
    mavenCentral()
}

dependencies {
    implementation(libs.kotlin.stdlib)
    implementation(libs.androidx.annotation)
    compileOnly(libs.jna) // element x provides JNA
}

android {
    namespace = "io.element.neutrino"
    compileSdk = 36

    defaultConfig {
        minSdk = 21
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlin {
        compilerOptions {
            jvmTarget = JvmTarget.JVM_17
        }
    }

    publishing {
        singleVariant("release")
    }
}

base {
    archivesName.set("neutrino")
}


publishing {
    publications {
        create<MavenPublication>("release") {
            groupId = "io.element.neutrino"
            artifactId = "bindings"
            version = "0.1.0"

            afterEvaluate {
                from(components["release"])
            }

            pom {
                name.set("Neutrino")
                description.set("Lightweight, embeddable homeserver written in Rust")

                licenses {
                    license {
                        name.set("AGPL-3.0-only OR LicenseRef-Element-Commercial")
                        url.set("https://www.gnu.org/licenses/agpl-3.0.txt")
                    }
                }
            }
        }
    }
}
