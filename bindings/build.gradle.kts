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

    // Required by the bundled `blew` BLE manager classes (org.jakebot.blew.*),
    // which back the iroh-over-BLE federation transport. Versions match blew's
    // own android module (blew-0.2.3/android/build.gradle.kts).
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    implementation("androidx.core:core-ktx:1.9.0")
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
            version = (findProperty("neutrinoVersion") as String?) ?: "0.1.0-SNAPSHOT"

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

    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/element-hq/neutrino")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}
