package io.vertx.ext.sync;

import co.paralleluniverse.fibers.*;
import co.paralleluniverse.strands.Strand;
import co.paralleluniverse.strands.SuspendableAction1;
import co.paralleluniverse.strands.SuspendableIterator;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.concurrent.ReentrantLock;
import com.google.common.base.Throwables;
import io.vertx.core.*;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.sync.concurrent.SuspendableFunction;
import io.vertx.ext.sync.impl.AsyncAdaptor;
import io.vertx.ext.sync.impl.HandlerAdaptor;
import io.vertx.ext.sync.impl.HandlerReceiverAdaptorImpl;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class contains various static methods to allowing events and asynchronous results to be accessed in a
 * synchronous way.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="https://benberman.com">Benjamin Berman</a>
 */
public class Sync {
  private static final String FIBER_SCHEDULER_CONTEXT_KEY = "__vertx-sync.fiberScheduler";
  private static final int DEFAULT_STACK_SIZE = 32;
  private static final String[] EXCLUDE_PREFIXES_FROM_STACK = new String[]{
    "co.paralleluniverse.fibers.",
    "co.paralleluniverse.strands.",
    "io.vertx.ext.sync.",
    "io.vertx.core.impl.future.",
    "io.vertx.core.Promise",
    "io.netty.",
    "io.vertx.core.impl.",
    "sun.nio.",
    "java.base/java.util.concurrent",
    "java.base/java.lang.Thread",
    "java.util.concurrent",
  };

  /**
   * Invoke an asynchronous operation and obtain the result synchronous. The fiber will be blocked until the result is
   * available. No kernel thread is blocked.
   *
   * @param consumer this should encapsulate the asynchronous operation. The handler is passed to it.
   * @param <T>      the type of the result
   * @return the result
   * @deprecated Use {@link #await(Future)} instead.
   */
  @Suspendable
  @Deprecated(forRemoval = true, since = "1.0.0")
  public static <T> T await(Consumer<Handler<AsyncResult<T>>> consumer) {
    try {
      return new AsyncAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          super.requestAsync();
          consumer.accept(this);
        }
      }.run();
    } catch (Throwable t) {
      throw makeSafe(t);
    }
  }

  /**
   * Awaits the completion of a promise using its {@link Promise#future()}.
   *
   * @param promise The promise
   * @param <T>     Its return type
   * @return The result.
   * @see #await(Future) for an explanation for the way this method throws checked exceptions.
   */
  @Suspendable
  public static <T> T await(Promise<T> promise) {
    return await(promise.future());
  }

  /**
   * Awaits the completion or failure of a Vertx {@code future}.
   *
   * @param future A vertx future
   * @param <T>    The return type of the future
   * @return A result
   * @throws RuntimeException when the future fails. Checked exceptions like {@link InterruptedException} and {@link
   *                          java.util.concurrent.TimeoutException} are both wrapped as the {@code cause} of the thrown
   *                          {@link RuntimeException}. Retrieve the root exception using {@link
   *                          com.google.common.base.Throwables#getRootCause(Throwable)}. The stack trace of the thrown
   *                          exception (i.e. the future's cause of failure) will be concatenated with the stack of the
   *                          site where {@code await} is called, and instrumented code noise in the stack trace will be
   *                          filtered out.
   */
  @Suspendable
  @NotNull
  public static <T> T await(Future<T> future) {
    if (!Strand.isCurrentFiber()) {
      try {
        return future.toCompletionStage().toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw makeSafe(e);
      }
    }
    try {
      return new AsyncAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          super.requestAsync();
          future.onComplete(this);
        }
      }.run();
    } catch (Throwable t) {
      throw makeSafe(t);
    }
  }

  /**
   * Awaits the completion or failure of a Vertx {@code future} until the specified {@code timeout}
   *
   * @param future   A vertx future
   * @param <T>      The return type of the future
   * @param timeout  the timeout to wait
   * @param timeUnit the unit of the timeout
   * @return A result
   * @throws RuntimeException when the future fails. Checked exceptions like {@link InterruptedException} and {@link
   *                          java.util.concurrent.TimeoutException} are both wrapped as the {@code cause} of the thrown
   *                          {@link RuntimeException}. Retrieve the root exception using {@link
   *                          com.google.common.base.Throwables#getRootCause(Throwable)}. The stack trace of the thrown
   *                          exception (i.e. the future's cause of failure) will be concatenated with the stack of the
   *                          site where {@code await} is called, and instrumented code noise in the stack trace will be
   *                          filtered out.
   */
  @Suspendable
  @NotNull
  public static <T> T await(Future<T> future, long timeout, TimeUnit timeUnit) {
    if (!Strand.isCurrentFiber()) {
      try {
        return future.toCompletionStage().toCompletableFuture().get(timeout, timeUnit);
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        throw makeSafe(e);
      }
    }
    try {
      return new AsyncAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          super.requestAsync();
          future.onComplete(this);
        }
      }.run(timeout, timeUnit);
    } catch (Throwable t) {
      throw makeSafe(t);
    }
  }

  /**
   * Invoke an asynchronous operation and obtain the result synchronous. The fiber will be blocked until the result is
   * available. No kernel thread is blocked. The consumer will be called inside a Fiber.
   *
   * @param consumer this should encapsulate the asynchronous operation. The handler is passed to it.
   * @param <T>      the type of the result
   * @return the result
   * @deprecated Use {@link #await(Future)} instead
   */
  @Suspendable
  @Deprecated(forRemoval = true, since = "1.0.0")
  public static <T> T awaitInFiber(Consumer<Handler<AsyncResult<T>>> consumer) {
    try {
      return new AsyncAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          if (Fiber.isCurrentFiber()) {
            try {
              super.requestAsync();
              consumer.accept(this);
            } catch (Exception e) {
              throw makeSafe(e);
            }
          } else {
            fiberHandler((Handler<Void> ignored) -> {
              super.requestAsync();
              consumer.accept(this);
            }).handle(null);
          }
        }
      }.run();
    } catch (Throwable t) {
      throw makeSafe(t);
    }
  }

  /**
   * Converts checked exceptions into runtime exceptions, and concatenates the stacktrace of the site where this is
   * called to the error passed to it.
   *
   * @param exception
   * @return
   */
  private static RuntimeException makeSafe(Throwable exception) {
    Throwable res = Throwables.getRootCause(exception);
    RuntimeException thrower;
    if (!(res instanceof RuntimeException)) {
      thrower = new RuntimeException(res);
    } else {
      thrower = (RuntimeException) res;
    }
    // Append the current stack so we see what called await fiber
    thrower.setStackTrace(concatAndFilterStackTrace(res, new Throwable()));
    return thrower;
  }

  /**
   * Concatenates and filters the stack traces of the provided {@link Throwables} instances.
   *
   * @param throwables whose stacktraces should be concatenated
   * @return a concatenated stack trace
   */
  public static StackTraceElement[] concatAndFilterStackTrace(Throwable... throwables) {
    int length = 0;
    for (int i = 0; i < throwables.length; i++) {
      length += throwables[i].getStackTrace().length;
    }
    ArrayList<StackTraceElement> newStack = new ArrayList<StackTraceElement>(length);
    for (Throwable throwable : throwables) {
      StackTraceElement[] stack = throwable.getStackTrace();
      toIterateTheStackTrace:
      for (StackTraceElement stackTraceElement : stack) {
        for (String anExcludedPrefix : EXCLUDE_PREFIXES_FROM_STACK) {
          if (stackTraceElement.getClassName().startsWith(anExcludedPrefix)) {
            continue toIterateTheStackTrace;
          }
        }
        newStack.add(stackTraceElement);
      }
    }
    return newStack.toArray(new StackTraceElement[0]);
  }

  /**
   * Invoke an asynchronous operation and obtain the result synchronous. The fiber will be blocked until the result is
   * available. No kernel thread is blocked.
   *
   * @param consumer this should encapsulate the asynchronous operation. The handler is passed to it.
   * @param timeout  In milliseconds when to cancel the awaited result
   * @param <T>      the type of the result
   * @return the result or null in case of a time out
   * @deprecated use
   */
  @Suspendable
  @Deprecated(forRemoval = true, since = "1.0.0")
  public static <T> T await(Consumer<Handler<AsyncResult<T>>> consumer, long timeout) {
    try {
      return new AsyncAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          try {
            super.requestAsync();
            consumer.accept(this);
          } catch (Exception e) {
            throw new VertxException(e);
          }
        }
      }.run(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException to) {
      return null;
    } catch (Throwable t) {
      throw new VertxException(t);
    }
  }

  /**
   * Receive a single event from a handler synchronously. The fiber will be blocked until the event occurs. No kernel
   * thread is blocked.
   *
   * @param consumer this should encapsulate the setting of the handler to receive the event. The handler is passed to
   *                 it.
   * @param <T>      the type of the event
   * @return the event
   * @deprecated use {@link #await(Future)} instead
   */
  @Suspendable
  @Deprecated(forRemoval = true, since = "1.0.0")
  public static <T> T awaitEvent(Consumer<Handler<T>> consumer) {
    try {
      return new HandlerAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          try {
            consumer.accept(this);
          } catch (Exception e) {
            throw makeSafe(e);
          }
        }
      }.run();
    } catch (Throwable t) {
      throw makeSafe(t);
    }
  }

  /**
   * Receive a single event from a handler synchronously. The fiber will be blocked until the event occurs. No kernel
   * thread is blocked.
   *
   * @param consumer this should encapsulate the setting of the handler to receive the event. The handler is passed to
   *                 it.
   * @param timeout  In milliseconds when to cancel the awaited event
   * @param <T>      the type of the event
   * @return the event
   * @deprecated use {@link #await(Future, long, TimeUnit)} instead
   */
  @Suspendable
  @Deprecated(forRemoval = true, since = "1.0.0")
  public static <T> T awaitEvent(Consumer<Handler<T>> consumer, long timeout) {
    try {
      return new HandlerAdaptor<T>() {
        @Override
        @Suspendable
        protected void requestAsync() {
          try {
            consumer.accept(this);
          } catch (Exception e) {
            throw makeSafe(e);
          }
        }
      }.run(timeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException to) {
      return null;
    } catch (Throwable t) {
      throw makeSafe(t);
    }
  }

  /**
   * Convert a standard handler to a handler which runs on a fiber. This is necessary if you want to do fiber blocking
   * synchronous operations in your handler.
   *
   * @param handler the standard handler
   * @param <T>     the event type of the handler
   * @return a wrapped handler that runs the handler on a fiber
   * @deprecated use {{@link #async(SuspendableAction1)}} for handlers instead
   */
  @Suspendable
  @Deprecated(since = "1.0.0", forRemoval = true)
  public static <T> Handler<T> fiberHandler(Handler<T> handler) {
    FiberScheduler scheduler = getContextScheduler();
    return p -> new Fiber<Void>(null, scheduler, DEFAULT_STACK_SIZE, () -> handler.handle(p)).start();
  }

  /**
   * Create an adaptor that converts a stream of events from a handler into a receiver which allows the events to be
   * received synchronously.
   *
   * @param <T> the type of the event
   * @return the adaptor
   */
  @Suspendable
  public static <T> HandlerReceiverAdaptor<T> streamAdaptor() {
    return new HandlerReceiverAdaptorImpl<>(getContextScheduler());
  }

  /**
   * Like {@link #streamAdaptor()} but using the specified Quasar {@link Channel} instance. This is useful if you want
   * to fine-tune the behaviour of the adaptor.
   *
   * @param channel the Quasar channel
   * @param <T>     the type of the event
   * @return the adaptor
   */
  @Suspendable
  public static <T> HandlerReceiverAdaptor<T> streamAdaptor(Channel<T> channel) {
    return new HandlerReceiverAdaptorImpl<>(getContextScheduler(), channel);
  }

  /**
   * Get the `FiberScheduler` for the current context. There should be only one instance per context.
   *
   * @return the scheduler
   */
  @Suspendable
  public static FiberScheduler getContextScheduler() {
    Context context = Vertx.currentContext();
    return getContextScheduler(context);
  }

  /**
   * Returns a fiber scheduler for the current context.
   * <p>
   * Fibers do not run concurrently with respect to one another on the same context.
   *
   * @param context
   * @return
   */
  @Suspendable
  public static FiberScheduler getContextScheduler(Context context) {
    if (context == null) {
      throw new IllegalStateException("Not in context");
    }
    if (!context.isEventLoopContext()) {
      throw new IllegalStateException("Not on event loop");
    }
    // We maintain one scheduler per context
    FiberScheduler scheduler = context.get(FIBER_SCHEDULER_CONTEXT_KEY);
    if (scheduler == null) {
      Thread eventLoop = Thread.currentThread();
      scheduler = new FiberExecutorScheduler("vertx.contextScheduler", command -> {
        if (Thread.currentThread() != eventLoop) {
          context.runOnContext(v -> command.run());
        } else {
          // Just run directly
          command.run();
        }
      });
      context.put(FIBER_SCHEDULER_CONTEXT_KEY, scheduler);
    }
    return scheduler;
  }

  /**
   * Remove the scheduler for the current context
   */
  @Suspendable
  public static void removeContextScheduler() {
    Context context = Vertx.currentContext();
    if (context != null) {
      context.remove(FIBER_SCHEDULER_CONTEXT_KEY);
    }
  }

  /**
   * Returns a handler that processes the incoming handler on the Vertx context.
   *
   * @param handler
   * @param context
   * @param <T>
   * @return
   */
  public static <T> BiConsumer<T, Throwable> resultHandler(Handler<AsyncResult<T>> handler, Context context) {
    checkNotNull(handler, "handler cannot be null");
    checkNotNull(context, "context cannot be null");
    return (result, error) -> {
      if (error == null) {
        context.runOnContext(v -> Future.succeededFuture(result).onComplete(handler));
      } else {
        context.runOnContext(v -> Future.<T>failedFuture(error).onComplete(handler));
      }
    };
  }

  /**
   * Returns a handler that executes inside a handler.
   * <p>
   * This allows you to use {@link CheckedSync#await(Future)} and {@link Sync#await(Future)}.
   *
   * @param handler A fiber-instrumented / fiber-safe handler
   * @param <T>     The incoming argument of the handler
   * @return a fiber handler
   */
  public static <T> Handler<T> async(SuspendableAction1<T> handler) {
    FiberScheduler scheduler = getContextScheduler();
    return async(scheduler, handler);
  }


  /**
   * Returns a mapper for {@link Future#compose(Function)} that runs {@code mapper} inside a fiber.
   * <p>
   * This permits you to run {@link #await(Future)} idiomatically with existing futures-style compose code:
   * <p>
   * {@code return future .compose(async(incoming -> { return await(something); })); }
   *
   * @param mapper A mapper
   * @param <T>    The incoming type from the previous future
   * @param <R>    The return type of this mapper
   * @return The decorated mapper
   */
  public static <T, R> Function<T, Future<R>> async(SuspendableFunction<T, R> mapper) {
    return (previousResult) -> {
      Promise<R> promise = Promise.<R>promise();
      Fiber<Object> fiber = new Fiber<>(getContextScheduler(), () -> {
        try {
          promise.complete(mapper.apply(previousResult));
        } catch (Throwable t) {
          promise.tryFail(t);
        }
      });
      fiber.setUncaughtExceptionHandler((f, e) -> promise.tryFail(e));
      fiber.inheritThreadLocals();
      fiber.start();
      return promise.future();
    };
  }


  private static void uncaughtException(Strand f, Throwable e) {
    Context currentContext = Vertx.currentContext();
    if (currentContext == null) {
      return;
    }
    Handler<Throwable> throwableHandler = currentContext.owner().exceptionHandler();
    if (throwableHandler == null) {
      return;
    }
    throwableHandler.handle(e);
  }


  private static <T> Handler<T> async(FiberScheduler scheduler, SuspendableAction1<T> handler) {
    return v -> {
      FiberScheduler passedScheduler = scheduler;
      if (passedScheduler == null) {
        passedScheduler = getContextScheduler();
      }
      Fiber<Void> fiber = new Fiber<Void>(passedScheduler, () -> handler.call(v));
      fiber.inheritThreadLocals();
      fiber.setUncaughtExceptionHandler(Sync::uncaughtException);
      fiber.start();
    };
  }


	/* todo: fix suspendable iterators
	@Suspendable
	public static <T> Iterable<T> await(ReadStream<T> stream) {
		return new ReadStreamIterable<>(stream);
	}*/

  /**
   * Awaits the completion or failure of a Java {@code CompletableFuture}.
   * <p>
   * Requires a non-null Vertx context, because it returns to the current context when the future completes.
   *
   * @param future A Java future
   * @param <T>    The return type of the future
   * @return A result
   * @throws RuntimeException when the future fails. Checked exceptions like {@link InterruptedException} and {@link
   *                          java.util.concurrent.TimeoutException} are both wrapped as the {@code cause} of the thrown
   *                          {@link RuntimeException}. Retrieve the root exception using {@link
   *                          com.google.common.base.Throwables#getRootCause(Throwable)}. The stack trace of the thrown
   *                          exception (i.e. the future's cause of failure) will be concatenated with the stack of the
   *                          site where {@code await} is called, and instrumented code noise in the stack trace will be
   *                          filtered out.
   */
  @Suspendable
  public static <T> T get(CompletableFuture<T> future) {
    Context context = Vertx.currentContext();
    return Sync.await(h -> future.whenComplete(resultHandler(h, context)));
  }

  @Suspendable
  private static class ReadStreamIterable<T> implements Iterable<T> {
    protected final Promise<Void> ended;
    protected final HandlerReceiverAdaptor<T> adaptor;

    public ReadStreamIterable(ReadStream<T> stream) {
      this.ended = Promise.<Void>promise();
      stream.endHandler(ended::tryComplete);
      stream.exceptionHandler(ended::tryFail);
      this.adaptor = Sync.streamAdaptor();
      stream.handler(adaptor);
      ended.future().onComplete(v -> adaptor.receivePort().close());
    }

    @NotNull
    @Override
    @Suspendable
    public SuspendableIterator<T> iterator() {
      return new ReadStreamIterator<T>(this);
    }

    @Override
    @Suspendable
    public void forEach(Consumer<? super T> action) {
      Objects.requireNonNull(action);
      for (T t : this) {
        action.accept(t);
      }
    }

  }

  @Suspendable
  private static class ReadStreamIterator<T> implements SuspendableIterator<T> {
    private final ReadStreamIterable<T> readStreamIterable;
    private final ReentrantLock lock = new ReentrantLock();
    private T next;
    private boolean completed;

    public ReadStreamIterator(ReadStreamIterable<T> readStreamIterable) {
      this.readStreamIterable = readStreamIterable;
    }

    private boolean isFailed() {
      return future().failed();
    }

    private Future<Void> future() {
      return readStreamIterable.ended.future();
    }

    @Override
    @Suspendable
    public boolean hasNext() {
      lock.lock();
      try {
        if (isFailed()) {
          throw makeSafe(future().cause());
        }

        // queued up but not yet consumed
        if (next != null) {
          return true;
        }

        if (completed) {
          return false;
        }

        // this will return null if the stream is ended
        T receive = readStreamIterable.adaptor.receive();
        if (receive == null) {
          readStreamIterable.ended.tryComplete();
          completed = readStreamIterable.adaptor.receivePort().isClosed();
          return false;
        }

        next = receive;
        return true;
      } finally {
        lock.unlock();
      }
    }

    @Override
    @Suspendable
    public T next() {
      lock.lock();
      try {
        if (isFailed()) {
          throw makeSafe(future().cause());
        }

        // already set by hasNext
        if (next != null) {
          T receive = next;
          next = null;
          return receive;
        } else {
          // closed peacefully
          if (completed) {
            throw new NoSuchElementException();
          }

          T receive = readStreamIterable.adaptor.receive();
          if (receive == null) {
            readStreamIterable.ended.tryComplete();
            completed = readStreamIterable.adaptor.receivePort().isClosed();
            throw new NoSuchElementException();
          }
          return receive;
        }
      } finally {
        lock.unlock();
      }
    }
  }
}
