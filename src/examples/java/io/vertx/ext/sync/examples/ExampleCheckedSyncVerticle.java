package io.vertx.ext.sync.examples;

import co.paralleluniverse.fibers.SuspendExecution;
import com.google.common.base.Throwables;
import com.google.common.collect.Streams;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.sync.SyncVerticle;
import io.vertx.ext.web.Router;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.vertx.ext.sync.CheckedSync.await;
import static io.vertx.ext.sync.Sync.async;

/**
 * Demonstrates a verticle that checks that await is only used inside suspendable (i.e. fibers-instrumented) functions.
 * <p>
 * Use {@code import static io.vertx.ext.sync.CheckedSync.await; } to get a version of {@link
 * io.vertx.ext.sync.CheckedSync#await(Future)} that requires you to call it inside a fibers-instrumented method.
 * <p>
 * Use {@code import static io.vertx.ext.sync.Sync.async; } to decorate a {@link io.vertx.core.Handler} to allow it to
 * call fibers sync code.
 */
public class ExampleCheckedSyncVerticle extends SyncVerticle {
  private HttpServer httpServer;
  private PgPool pgPool;

  @Override
  protected void syncStart() throws SuspendExecution, InterruptedException {
    httpServer = vertx.createHttpServer();
    pgPool = PgPool.pool();

    Router router = Router.router(vertx);
    router.route("/test")
      // observe the use of the async keyword to decorate a typical handler
      // this lets you use await to await the completion of vertx Futures
      .handler(async(routingContext -> {
        // you can now use await here
        SqlConnection connection = await(pgPool.getConnection());

        try {
          // observe the use of a timeout
          RowSet<Row> result = await(connection
            .query("select * from very_large_table")
            .execute(), 1000L, TimeUnit.MILLISECONDS);

          String data = Streams.stream(result).map(row -> row.getString("some_column"))
            .collect(Collectors.joining(","));

          await(routingContext.end(data));
        } catch (Throwable possiblyCheckedException) {
          // example showing how to handle timeout
          Throwable rootCause = Throwables.getRootCause(possiblyCheckedException);
          if (rootCause instanceof TimeoutException) {
            routingContext.response().setStatusCode(408);
            routingContext.end("request timed out");
          } else {
            routingContext.response().setStatusCode(503);
            routingContext.end("internal error occurred");
          }
        } finally {
          await(connection.close());
        }
      }));

    httpServer.requestHandler(router);

    // in a sync verticle, syncStart and syncStop are fibers-safe
    await(httpServer.listen());
  }

  @Override
  protected void syncStop() throws SuspendExecution {
    await(pgPool.close());
    await(httpServer.close());
  }
}
