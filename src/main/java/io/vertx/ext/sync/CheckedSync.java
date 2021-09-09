package io.vertx.ext.sync;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.SuspendableAction1;
import io.vertx.core.Future;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CheckedSync {
  /**
   * Awaits the completion or failure of a Vertx {@code future}.
   * <p>
   *
   * @param future A vertx future
   * @param <T>    The return type of the future
   * @return A result
   * @throws SuspendExecution indicating this method must appear inside a fiber or the body of an {@link
   *                          Sync#async(SuspendableAction1)} handler
   * @throws RuntimeException when the future fails. Checked exceptions like {@link InterruptedException} and {@link
   *                          java.util.concurrent.TimeoutException} are both wrapped as the {@code cause} of the thrown
   *                          {@link RuntimeException}. Retrieve the root exception using {@link
   *                          com.google.common.base.Throwables#getRootCause(Throwable)}. The stack trace of the thrown
   *                          exception (i.e. the future's cause of failure) will be concatenated with the stack of the
   *                          site where {@code await} is called, and instrumented code noise in the stack trace will be
   *                          filtered out.
   */
  public static <T> T await(Future<T> future) throws SuspendExecution {
    return Sync.await(future);
  }

  /**
   * Awaits the completion or failure of a Java {@code CompletableFuture}.
   * <p>
   *
   * @param future A Java future
   * @param <T>    The return type of the future
   * @return A result
   * @throws SuspendExecution indicating this method must appear inside a fiber or the body of an {@link
   *                          Sync#async(SuspendableAction1)} handler
   * @throws RuntimeException when the future fails. Checked exceptions like {@link InterruptedException} and {@link
   *                          java.util.concurrent.TimeoutException} are both wrapped as the {@code cause} of the thrown
   *                          {@link RuntimeException}. Retrieve the root exception using {@link
   *                          com.google.common.base.Throwables#getRootCause(Throwable)}. The stack trace of the thrown
   *                          exception (i.e. the future's cause of failure) will be concatenated with the stack of the
   *                          site where {@code await} is called, and instrumented code noise in the stack trace will be
   *                          filtered out.
   */
  public static <T> T await(CompletableFuture<T> future) throws SuspendExecution {
    return Sync.get(future);
  }

  /**
   * Awaits the completion or failure of a Vertx {@code future} until the specified {@code timeout}
   *
   * @param future   A vertx future
   * @param <T>      The return type of the future
   * @param timeout  the timeout to wait
   * @param timeUnit the unit of the timeout
   * @throws SuspendExecution indicating this method must appear inside a fiber or the body of an {@link
   *                          Sync#async(SuspendableAction1)} handler
   * @throws RuntimeException when the future fails. Checked exceptions like {@link InterruptedException} and {@link
   *                          java.util.concurrent.TimeoutException} are both wrapped as the {@code cause} of the thrown
   *                          {@link RuntimeException}. Retrieve the root exception using {@link
   *                          com.google.common.base.Throwables#getRootCause(Throwable)}. The stack trace of the thrown
   *                          exception (i.e. the future's cause of failure) will be concatenated with the stack of the
   *                          site where {@code await} is called, and instrumented code noise in the stack trace will be
   *                          filtered out.
   */
  @NotNull
  public static <T> T await(Future<T> future, long timeout, TimeUnit timeUnit) throws SuspendExecution {
    return Sync.await(future, timeout, timeUnit);
  }
}
