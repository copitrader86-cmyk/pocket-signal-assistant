import java.net.URI

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.compose")
}

val apiBaseUrl = providers.gradleProperty("apiBaseUrl")
    .orElse("http://10.0.2.2:8000")
    .get()
val releaseApiBaseUrl = providers.gradleProperty("releaseApiBaseUrl")
    .orElse("")
    .get()
val releaseStoreFile = providers.gradleProperty("releaseStoreFile").orNull
val releaseStorePassword = providers.gradleProperty("releaseStorePassword").orNull
val releaseKeyAlias = providers.gradleProperty("releaseKeyAlias").orNull
val releaseKeyPassword = providers.gradleProperty("releaseKeyPassword").orNull

android {
    namespace = "com.pocketsignal"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.pocketsignal"
        minSdk = 26
        targetSdk = 36
        versionCode = 1
        versionName = "0.1.0"
        buildConfigField("String", "API_BASE_URL", "\"$apiBaseUrl\"")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    buildFeatures {
        compose = true
        buildConfig = true
    }

    buildTypes {
        getByName("release") {
            buildConfigField("String", "API_BASE_URL", "\"$releaseApiBaseUrl\"")
            signingConfig = signingConfigs.create("release").apply {
                storeFile = releaseStoreFile?.let(::file)
                storePassword = releaseStorePassword
                keyAlias = releaseKeyAlias
                keyPassword = releaseKeyPassword
            }
        }
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

dependencies {
    implementation(platform("androidx.compose:compose-bom:2026.06.01"))
    implementation("androidx.activity:activity-compose:1.13.0")
    implementation("androidx.compose.foundation:foundation")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.10.0")
    implementation("com.squareup.okhttp3:okhttp:5.3.2")

    debugImplementation("androidx.compose.ui:ui-tooling")
}

val validateReleaseApiUrl by tasks.registering {
    doLast {
        val uri = runCatching { URI(releaseApiBaseUrl) }.getOrNull()
        require(
            uri?.let {
                it.scheme == "https" &&
                    !it.host.isNullOrBlank() &&
                    it.host.lowercase() !in setOf(
                        "localhost",
                        "127.0.0.1",
                        "10.0.2.2",
                        "example.com",
                        "example.org",
                    ) &&
                    listOf(".localhost", ".local", ".test", ".example", ".invalid")
                        .none(it.host.lowercase()::endsWith)
            } == true,
        ) {
            "Release builds require -PreleaseApiBaseUrl=https://<public-server-host>; local/test URLs are not allowed."
        }
        require(
            !releaseStoreFile.isNullOrBlank() &&
                !releaseStorePassword.isNullOrBlank() &&
                !releaseKeyAlias.isNullOrBlank() &&
                !releaseKeyPassword.isNullOrBlank() &&
                file(releaseStoreFile).isFile,
        ) {
            "Release builds require a valid signing keystore and release signing properties."
        }
    }
}

tasks.configureEach {
    if (name != "validateReleaseApiUrl" && name.contains("Release", ignoreCase = true)) {
        dependsOn(validateReleaseApiUrl)
    }
}
