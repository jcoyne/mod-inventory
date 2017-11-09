package org.folio.inventory.resources.ingest;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.folio.inventory.CollectionResourceClient;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.ingest.IngestMessages;
import org.folio.inventory.parsing.ModsParser;
import org.folio.inventory.parsing.UTF8LiteralCharacterEncoding;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.*;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ModsIngestion {
  private final Storage storage;

  public ModsIngestion(final Storage storage) {
    this.storage = storage;
  }

  public void register(Router router) {
    router.post(relativeModsIngestPath() + "*").handler(BodyHandler.create());
    router.post(relativeModsIngestPath()).handler(this::ingest);
    router.get(relativeModsIngestPath() + "/status/:id").handler(this::status);
  }

  //TODO: Will only work for book material type
  // can circulate loan type
  // and main library location
  private void ingest(RoutingContext routingContext) {
    if(routingContext.fileUploads().size() > 1) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Cannot parse multiple files in a single request");
      return;
    }

    WebContext context = new WebContext(routingContext);
    OkapiHttpClient client;
    CollectionResourceClient materialTypesClient;
    CollectionResourceClient loanTypesClient;
    CollectionResourceClient locationsClient;
    CollectionResourceClient instanceTypesClient;
    CollectionResourceClient identifierTypesClient;
    CollectionResourceClient creatorTypesClient;

    try {
      client = createHttpClient(routingContext, context);

      materialTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/material-types"));

      loanTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/loan-types"));

      locationsClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/shelf-locations"));

      identifierTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/identifier-types"));

      instanceTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/instance-types"));

      creatorTypesClient = new CollectionResourceClient(client,
        new URL(context.getOkapiLocation() + "/creator-types"));
    }
    catch (MalformedURLException e) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));

      return;
    }

    String materialTypesQuery = null;
    String loanTypesQuery = null;
    String locationsQuery = null;
    String instanceTypesQuery = null;
    String identifierTypesQuery = null;
    String creatorTypesQuery = null;

    try {
      materialTypesQuery = "query=" + URLEncoder.encode("name=\"Book\"", "UTF-8");
      loanTypesQuery = "query=" + URLEncoder.encode("name=\"Can Circulate\"", "UTF-8");
      locationsQuery = "query=" + URLEncoder.encode("name=\"Main Library\"", "UTF-8");
      instanceTypesQuery = "query=" + URLEncoder.encode("name=\"Books\"", "UTF-8");
      identifierTypesQuery = "query=" + URLEncoder.encode("name=\"ISBN\"", "UTF-8");
      creatorTypesQuery = "query=" + URLEncoder.encode("name=\"Personal name\"", "UTF-8");

    } catch (UnsupportedEncodingException e) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to encode query"));
    }

    CompletableFuture<Response> materialTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> loanTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> locationsRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> instanceTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> identifierTypesRequestCompleted = new CompletableFuture<>();
    CompletableFuture<Response> creatorTypesRequestCompleted = new CompletableFuture<>();

    materialTypesClient.getMany(
      materialTypesQuery,
      materialTypesRequestCompleted::complete);

    loanTypesClient.getMany(
      loanTypesQuery,
      loanTypesRequestCompleted::complete);

    locationsClient.getMany(
      locationsQuery,
      locationsRequestCompleted::complete);

    instanceTypesClient.getMany(
      instanceTypesQuery,
      instanceTypesRequestCompleted::complete);

    identifierTypesClient.getMany(
      identifierTypesQuery,
      identifierTypesRequestCompleted::complete);

    creatorTypesClient.getMany(
      creatorTypesQuery,
      creatorTypesRequestCompleted::complete);

    CompletableFuture.allOf(materialTypesRequestCompleted,
      loanTypesRequestCompleted, locationsRequestCompleted,
      instanceTypesRequestCompleted, identifierTypesRequestCompleted,
      creatorTypesRequestCompleted)
      .thenAccept(v -> {
        Response materialTypeResponse = materialTypesRequestCompleted.join();
        Response loanTypeResponse = loanTypesRequestCompleted.join();
        Response locationsResponse = locationsRequestCompleted.join();
        Response instanceTypesResponse = instanceTypesRequestCompleted.join();
        Response identifierTypesResponse = identifierTypesRequestCompleted.join();
        Response creatorTypesResponse = creatorTypesRequestCompleted.join();

      if (materialTypeResponse.getStatusCode() != 200) {
        ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve material types: %s: %s",
          materialTypeResponse.getStatusCode(), materialTypeResponse.getBody()));

      return;
    }

    if (loanTypeResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve loan types: %s: %s",
          loanTypeResponse.getStatusCode(), loanTypeResponse.getBody()));

      return;
    }

    if (locationsResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve locations: %s: %s",
          locationsResponse.getStatusCode(), locationsResponse.getBody()));

      return;
    }

    if (instanceTypesResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve instance types: %s: %s",
          instanceTypesResponse.getStatusCode(), instanceTypesResponse.getBody()));

      return;
    }

    if (identifierTypesResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve identifier types: %s: %s",
          identifierTypesResponse.getStatusCode(), identifierTypesResponse.getBody()));

      return;
    }

    if (creatorTypesResponse.getStatusCode() != 200) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to retrieve creator types: %s: %s",
          creatorTypesResponse.getStatusCode(), creatorTypesResponse.getBody()));

      return;
    }

    List<JsonObject> materialTypes = JsonArrayHelper.toList(
      materialTypeResponse.getJson().getJsonArray("mtypes"));

    if(materialTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find book material type: %s", materialTypeResponse.getBody()));

      return;
    }

    List<JsonObject> loanTypes = JsonArrayHelper.toList(
      loanTypeResponse.getJson().getJsonArray("loantypes"));

    if(loanTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find can circulate loan type: %s", loanTypeResponse.getBody()));

      return;
    }

    List<JsonObject> locations = JsonArrayHelper.toList(
      locationsResponse.getJson().getJsonArray("shelflocations"));

    String bookMaterialTypeId = materialTypes.stream().findFirst().get().getString("id");
    String canCirculateLoanTypeId = loanTypes.stream().findFirst().get().getString("id");

    //location is optional so carry on gracefully
    String mainLibraryLocationId = locations.size() == 1
      ? locations.stream().findFirst().get().getString("id")
      : null;

    List<JsonObject> identifierTypes = JsonArrayHelper.toList(
      identifierTypesResponse.getJson().getJsonArray("identifierTypes"));

    if(identifierTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find ISBN identifier type: %s",
          identifierTypesResponse.getBody()));

      return;
    }

    String isbnIdentifierTypeId = identifierTypes.stream().findFirst().get().getString("id");

    List<JsonObject> instanceTypes = JsonArrayHelper.toList(
      instanceTypesResponse.getJson().getJsonArray("instanceTypes"));

    if(instanceTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find books instance type: %s",
          instanceTypesResponse.getBody()));

      return;
    }

    String booksInstanceTypeId = instanceTypes.stream().findFirst().get().getString("id");

    List<JsonObject> creatorTypes = JsonArrayHelper.toList(
      creatorTypesResponse.getJson().getJsonArray("creatorTypes"));

    if(creatorTypes.size() != 1) {
      ServerErrorResponse.internalError(routingContext.response(),
        String.format("Unable to find personal name creator type: %s",
          creatorTypesResponse.getBody()));

      return;
    }

    String personalCreatorTypeId = creatorTypes.stream().findFirst().get().getString("id");

    routingContext.vertx().fileSystem().readFile(uploadFileName(routingContext),
      result -> {
        if (result.succeeded()) {
          String uploadedFileContents = result.result().toString();

          try {
            List<JsonObject> records = new ModsParser(new UTF8LiteralCharacterEncoding())
              .parseRecords(uploadedFileContents);

            storage.getIngestJobCollection(context)
              .add(new IngestJob(IngestJobState.REQUESTED),
                success -> {
                  IngestMessages.start(records,
                    singleEntryMap("Book", bookMaterialTypeId),
                    singleEntryMap("Can Circulate", canCirculateLoanTypeId),
                    singleEntryMap("Main Library", mainLibraryLocationId),
                    singleEntryMap("ISBN", isbnIdentifierTypeId),
                    singleEntryMap("Books", booksInstanceTypeId),
                    singleEntryMap("Personal name", personalCreatorTypeId),
                    success.getResult().id, context).send(routingContext.vertx());

                  RedirectResponse.accepted(routingContext.response(),
                    statusLocation(routingContext, success.getResult().id));
                },
                failure -> System.out.println("Creating Ingest Job failed")
              );
          } catch (Exception e) {
            ServerErrorResponse.internalError(routingContext.response(),
              String.format("Unable to parse MODS file:%s", e.toString()));
          }
        } else {
        ServerErrorResponse.internalError(
          routingContext.response(), result.cause().toString());
        }
      });
    });
  }

  private void status(RoutingContext routingContext) {
    Context context = new WebContext(routingContext);

    storage.getIngestJobCollection(context)
      .findById(routingContext.request().getParam("id"),
        it -> JsonResponse.success(routingContext.response(),
          new JsonObject().put("status", it.getResult().state.toString())),
        FailureResponseConsumer.serverError(routingContext.response()));
  }

  private Map<String, String> singleEntryMap(String key, String value) {
    HashMap<String, String> map = new HashMap<>();

    map.put(key, value);

    return map;
  }

  private String statusLocation(RoutingContext routingContext, String jobId) {

    String scheme = routingContext.request().scheme();
    String host = routingContext.request().host();

    return String.format("%s://%s%s/status/%s",
      scheme, host, relativeModsIngestPath(), jobId);
  }

  private String uploadFileName(RoutingContext routingContext) {
    return routingContext.fileUploads().stream().findFirst().get()
      .uploadedFileName();
  }

  private static String relativeModsIngestPath() {
    return "/inventory/ingest/mods";
  }

  private OkapiHttpClient createHttpClient(RoutingContext routingContext,
                                           WebContext context)
    throws MalformedURLException {

    return new OkapiHttpClient(routingContext.vertx().createHttpClient(),
      new URL(context.getOkapiLocation()), context.getTenantId(),
      context.getToken(),
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to contact storage module: %s",
          exception.toString())));
  }
}
