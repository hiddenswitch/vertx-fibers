package io.vertx.ext.sync.concurrent.impl;

import co.paralleluniverse.fibers.Suspendable;
import io.vertx.core.Closeable;
import io.vertx.core.Promise;
import io.vertx.core.shareddata.Counter;
import io.vertx.ext.sync.Sync;
import io.vertx.ext.sync.concurrent.SuspendableCounter;

import static io.vertx.ext.sync.Sync.await;

public class SuspendableVertxCounter implements SuspendableCounter {
  private final Counter counter;

  public SuspendableVertxCounter(Counter counter) {
    this.counter = counter;
  }

  @Override
  @Suspendable
  public long get() {
    return Sync.await(counter::get);
  }

  @Override
  @Suspendable
  public long incrementAndGet() {
    return Sync.await(counter::incrementAndGet);
  }

  @Override
  @Suspendable
  public long getAndIncrement() {
    return Sync.await(counter::getAndIncrement);
  }

  @Override
  @Suspendable
  public long decrementAndGet() {
    return Sync.await(counter::decrementAndGet);
  }

  @Override
  @Suspendable
  public long addAndGet(long value) {
    return io.vertx.ext.sync.Sync.await(counter.addAndGet(value));
  }

  @Override
  @Suspendable
  public long getAndAdd(long value) {
    return io.vertx.ext.sync.Sync.await(counter.getAndAdd(value));
  }

  @Override
  @Suspendable
  public boolean compareAndSet(long expected, long value) {
    return io.vertx.ext.sync.Sync.await(counter.compareAndSet(expected, value));
  }

  @Override
  @Suspendable
  public void close() {
    if (counter instanceof Closeable) {
      Promise<Void> p = Promise.<Void>promise();
      ((Closeable) counter).close(p);
      await(p.future());
    }
  }
}
