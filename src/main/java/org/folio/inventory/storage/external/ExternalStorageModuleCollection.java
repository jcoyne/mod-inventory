package org.folio.inventory.storage.external;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.Response;
import org.folio.util.PercentCodec;

import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

abstract class ExternalStorageModuleCollection<T> {
  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleCollection.class);

  private final String storageAddress;
  private final String collectionWrapperPropertyName;
  private final WebClient webClient;
  private final Function<T, String> mapToId;
  protected final Function<T, JsonObject> mapToRequest;
  protected final Function<JsonObject, T> mapFromResponse;
  private final StandardHeaders standardHeaders;

  ExternalStorageModuleCollection(String storageAddress,
    String collectionWrapperPropertyName, HttpClient client,
    StandardHeaders standardHeaders, Function<T, String> mapToId,
    Function<T, JsonObject> mapToRequest, Function<JsonObject, T> mapFromResponse) {

    this.storageAddress = storageAddress;
    this.collectionWrapperPropertyName = collectionWrapperPropertyName;
    this.webClient = WebClient.wrap(client);
    this.mapToId = mapToId;
    this.mapToRequest = mapToRequest;
    this.mapFromResponse = mapFromResponse;
    this.standardHeaders = standardHeaders;
  }

  ExternalStorageModuleCollection(String storageAddress,
    String collectionWrapperPropertyName, StandardHeaders standardHeaders,
    HttpClient client, Function<T, String> mapToId, StorageMapper<T> storageMapper) {

    this(storageAddress, collectionWrapperPropertyName, client,
      standardHeaders, mapToId, storageMapper::mapToRequest,
      storageMapper::mapFromResponse);
  }

  public void add(T item, Consumer<Success<T>> resultCallback,
    Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final var request = createPostRequest(storageAddress);

    request.sendJsonObject(mapToRequest.apply(item), futureResponse::complete);

    futureResponse
      .thenCompose(asyncResult -> new ResponseMapper().mapAsyncResultToCompletionStage(asyncResult
      ))
      .thenAccept(response -> {
        if (response.getStatusCode() == 201) {
          try {
            T created = mapFromResponse.apply(response.getJson());
            resultCallback.accept(new Success<>(created));
          } catch (Exception e) {
            LOGGER.error(e);
            failureCallback.accept(new Failure(e.getMessage(), response.getStatusCode()));
          }
        } else {
          failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
        }
      });

  }

  public void findById(String id,
                       Consumer<Success<T>> resultCallback,
                       Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final var request = standardHeaders
      .applyTo(webClient.getAbs(individualRecordLocation(id)));

    request.send(futureResponse::complete);

    futureResponse
      .thenCompose(asyncResult -> new ResponseMapper().mapAsyncResultToCompletionStage(asyncResult
      ))
      .thenAccept(response -> {
        switch (response.getStatusCode()) {
          case 200:
            JsonObject instanceFromServer = response.getJson();

            try {
              T found = mapFromResponse.apply(instanceFromServer);
              resultCallback.accept(new Success<>(found));
              break;
            } catch (Exception e) {
              LOGGER.error(e);
              failureCallback.accept(new Failure(e.getMessage(), 500));
              break;
            }

          case 404:
            resultCallback.accept(new Success<>(null));
            break;

          default:
            failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
        }
      });
  }

  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) {

    String location = String.format("%s?limit=%s&offset=%s", storageAddress,
      pagingParameters.limit, pagingParameters.offset);

    find(location, resultCallback, failureCallback);
  }

  public void empty(
    String cqlQuery,
    Consumer<Success<Void>> completionCallback,
    Consumer<Failure> failureCallback) {

    if (cqlQuery == null) {
      failureCallback.accept(new Failure("query parameter is required", 400));
      return;
    }
    deleteLocation(storageAddress + "?query=" + PercentCodec.encode(cqlQuery), completionCallback, failureCallback);
  }

  public void findByCql(String cqlQuery,
                        PagingParameters pagingParameters,
                        Consumer<Success<MultipleRecords<T>>> resultCallback,
                        Consumer<Failure> failureCallback) {

    String encodedQuery = URLEncoder.encode(cqlQuery, StandardCharsets.UTF_8);

    String location =
      String.format("%s?query=%s", storageAddress, encodedQuery) +
        String.format("&limit=%s&offset=%s", pagingParameters.limit,
          pagingParameters.offset);

    find(location, resultCallback, failureCallback);
  }

  public void update(T item,
                     Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {

    String location = individualRecordLocation(mapToId.apply(item));

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final var request = standardHeaders.applyTo(webClient.putAbs(location));

    request.sendJsonObject(mapToRequest.apply(item), futureResponse::complete);

    futureResponse
      .thenCompose(asyncResult -> new ResponseMapper().mapAsyncResultToCompletionStage(asyncResult
      ))
      .thenAccept(response ->
        interpretNoContentResponse(response, completionCallback, failureCallback));
  }

  public void delete(String id, Consumer<Success<Void>> completionCallback,
                     Consumer<Failure> failureCallback) {

    deleteLocation(individualRecordLocation(id), completionCallback, failureCallback);
  }

  private String individualRecordLocation(String id) {
    return String.format("%s/%s", storageAddress, id);
  }

  private void find(String location,
    Consumer<Success<MultipleRecords<T>>> resultCallback,
    Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final var request = standardHeaders.applyTo(webClient.getAbs(location));

    request.send(futureResponse::complete);

    futureResponse
      .thenCompose(asyncResult -> new ResponseMapper().mapAsyncResultToCompletionStage(asyncResult
      ))
      .thenAccept(response ->
        interpretMultipleRecordResponse(resultCallback, failureCallback, response));
  }

  private void interpretMultipleRecordResponse(
    Consumer<Success<MultipleRecords<T>>> resultCallback, Consumer<Failure> failureCallback,
    Response response) {

    if (response.getStatusCode() == 200) {
      try {
        JsonObject wrappedRecords = response.getJson();

        List<JsonObject> records = JsonArrayHelper.toList(
          wrappedRecords.getJsonArray(collectionWrapperPropertyName));

        List<T> foundRecords = records.stream()
          .map(mapFromResponse)
          .collect(Collectors.toList());

        MultipleRecords<T> result = new MultipleRecords<>(
          foundRecords, wrappedRecords.getInteger("totalRecords"));

        resultCallback.accept(new Success<>(result));
      } catch (Exception e) {
        LOGGER.error(e);
        failureCallback.accept(new Failure(e.getMessage(), response.getStatusCode()));
      }

    } else {
      failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
    }
  }

  protected HttpRequest<Buffer> createPostRequest(String address) {
    return standardHeaders.applyTo(webClient.postAbs(address));
  }

  private void deleteLocation(String location, Consumer<Success<Void>> completionCallback,
                              Consumer<Failure> failureCallback) {

    final var futureResponse = new CompletableFuture<AsyncResult<HttpResponse<Buffer>>>();

    final var request = standardHeaders.applyTo(webClient.deleteAbs(location));

    request.send(futureResponse::complete);

    futureResponse
      .thenCompose(asyncResult -> new ResponseMapper().mapAsyncResultToCompletionStage(asyncResult
      ))
      .thenAccept(response ->
        interpretNoContentResponse(response, completionCallback, failureCallback));
  }

  private void interpretNoContentResponse(Response response, Consumer<Success<Void>> completionCallback, Consumer<Failure> failureCallback) {
    if (response.getStatusCode() == 204) {
      completionCallback.accept(new Success<>(null));
    } else {
      failureCallback.accept(new Failure(response.getBody(), response.getStatusCode()));
    }
  }
}
