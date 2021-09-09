package io.vertx.ext.sync;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

/**
 * A `Verticle` which runs its `start` and `stop` methods using fibers.
 * <p>
 * You should subclass this class instead of `AbstractVerticle` to create any verticles that use vertx-sync.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public abstract class SyncVerticle extends AbstractVerticle {

  private FiberScheduler instanceScheduler;


  @Override
  public final void start(Promise<Void> startFuture) throws Exception {
    instanceScheduler = Sync.getContextScheduler();
    new Fiber<Void>("starting verticle", instanceScheduler, () -> {
      try {
        syncStart();
        startFuture.complete();
      } catch (Throwable t) {
        startFuture.fail(t);
      }
    }).start();
  }

  @Override
  public final void stop(Promise<Void> stopFuture) throws Exception {
    new Fiber<Void>("stopping verticle", instanceScheduler, () -> {
      try {
        syncStop();
        stopFuture.complete();
      } catch (Throwable t) {
        stopFuture.fail(t);
      } finally {
        Sync.removeContextScheduler();
      }
    }).start();
  }

  /**
   * Override this method in your verticle
   */
  protected abstract void syncStart() throws SuspendExecution, InterruptedException;

  protected abstract void syncStop() throws SuspendExecution, InterruptedException;

  @Override
  public final void start() {
    throw new UnsupportedOperationException();
  }


  @Override
  public final void stop() {
    throw new UnsupportedOperationException();
  }
}
