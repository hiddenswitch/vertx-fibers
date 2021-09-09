# vert-fibers

This is a fork of Vertx's Sync library updated for Vertx 4.+, Java 11+ and with instrumentation plugins for Gradle.

### Overview

`Fiber` from [java-fibers](https://github.com/hiddenswitch/java-fibers) lets you call **async** code like `vertx` as
though it was synchronous code.

### Installation

**build.gradle**

```groovy
// currently served on jitpack
repositories {
  maven { url 'https://jitpack.io' }
}

// enable ahead of time instrumentation of your code
plugins {
  id 'com.hiddenswitch.fibers.instrument' version '1.0.3'
}

// configure your instrumentation
fibers {
  // shows errors that tell you what code is missing @Suspendable attributes
  check = true
}

dependencies {
  // include this library, transitively depends on com.hiddenswitch:quasar-core:10.0.3
  implementation 'com.github.hiddenswitch:vertx-fibers:master-SNAPSHOT'
  // include a version of vertx. tested with the latest 4.x series
  implementation 'io.vertx:vertx-core:4.0.0'
}
```

### Usage

See a detailed example
in [ExampleCheckedSyncVerticle.java](src/examples/java/io/vertx/ext/sync/examples/ExampleCheckedSyncVerticle.java).

Detailed documentation is coming soon.
