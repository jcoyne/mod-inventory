package org.folio.inventory.storage.external;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHeaders.LOCATION;

import java.util.concurrent.CompletionStage;

import org.folio.inventory.support.http.client.Response;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;

public class ResponseMapper {
  Response mapResponse(AsyncResult<HttpResponse<Buffer>> asyncResult) {
    final var response = asyncResult.result();

    return new Response(response.statusCode(), response.bodyAsString(),
      response.getHeader(CONTENT_TYPE), response.getHeader(LOCATION));
  }

  CompletionStage<Response> mapAsyncResultToCompletionStage(
    AsyncResult<HttpResponse<Buffer>> asyncResult) {

    return asyncResult.succeeded()
      ? completedFuture(mapResponse(asyncResult))
      : failedFuture(asyncResult.cause());
  }
}
