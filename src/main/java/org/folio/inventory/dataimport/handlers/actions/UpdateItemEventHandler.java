package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;
import static org.folio.inventory.domain.items.Item.STATUS_KEY;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import java.io.UnsupportedEncodingException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.entities.PartialError;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.JsonHelper;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.MappingContext;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

public class UpdateItemEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(UpdateItemEventHandler.class);
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update an Item requires a mapping profile";
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data or ITEM to update";
  private static final String STATUS_UPDATE_ERROR_MSG = "Could not change item status '%s' to '%s'";
  private static final String MAPPING_METADATA_NOT_FOUND_MSG = "MappingMetadata snapshot was not found by jobExecutionId '%s'. RecordId: '%s', chunkId: '%s' ";
  private static final String ITEM_PATH_FIELD = "item";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final Set<String> PROTECTED_STATUSES_FROM_UPDATE = new HashSet<>(Arrays.asList("Aged to lost", "Awaiting delivery", "Awaiting pickup", "Checked out", "Claimed returned", "Declared lost", "Paged", "Recently returned"));
  static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String ERRORS = "ERRORS";
  private static final String BLANK = "";
  private static final String BLANK_JSON_ARRAY = "[]";
  private static final String MULTIPLE_HOLDINGS_FIELD = "MULTIPLE_HOLDINGS_FIELD";
  private static final String TEMPORARY_MULTIPLE_HOLDINGS_FIELD = "TEMPORARY_MULTIPLE_HOLDINGS_FIELD";
  public static final String ID_PATH_FIELD = "id";
  private final List<String> requiredFields = Arrays.asList("status.name", "materialType.id", "permanentLoanType.id", "holdingsRecordId");
  private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneOffset.UTC);

  private final Storage storage;
  private final MappingMetadataCache mappingMetadataCache;

  public UpdateItemEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    this.storage = storage;
    this.mappingMetadataCache = mappingMetadataCache;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_ITEM_UPDATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))
        || isBlank(payloadContext.get(ITEM.value())) || new JsonArray(payloadContext.get(ITEM.value())).isEmpty()) {
        LOGGER.warn("handle:: " + PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.warn("handle:: " + ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }
      LOGGER.info("handle:: Processing UpdateItemEventHandler starting with jobExecutionId: {}.", dataImportEventPayload.getJobExecutionId());
      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
      String chunkId = dataImportEventPayload.getContext().get(CHUNK_ID_HEADER);

      mappingMetadataCache.get(jobExecutionId, context)
        .map(parametersOptional -> parametersOptional
          .orElseThrow(() -> new EventProcessingException(format(MAPPING_METADATA_NOT_FOUND_MSG, jobExecutionId,
            recordId, chunkId))))
        .onSuccess(mappingMetadataDto -> {
          if (dataImportEventPayload.getContext().containsKey(MULTIPLE_HOLDINGS_FIELD)) {
            dataImportEventPayload.getContext().put(TEMPORARY_MULTIPLE_HOLDINGS_FIELD, dataImportEventPayload.getContext().get(MULTIPLE_HOLDINGS_FIELD));
          }
          Map<String, String> oldItemStatuses = preparePayloadAndGetStatus(dataImportEventPayload);
          MappingParameters mappingParameters = Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class);
          MappingManager.map(dataImportEventPayload, new MappingContext().withMappingParameters(mappingParameters));

          ItemCollection itemCollection = storage.getItemCollection(context);
          List<Future> updatedItemsRecordFutures = new ArrayList<>();
          List<Item> updatedItemEntities = new ArrayList<>();
          List<PartialError> errors = new ArrayList<>();

          JsonArray itemsJsonArray = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
          LOGGER.trace(format("handle:: Mapped Items to update: %s", dataImportEventPayload.getContext().get(ITEM.value())));
          List<Item> expiredItems = new ArrayList<>();
          for (int i = 0; i < itemsJsonArray.size(); i++) {
            Promise<Void> updatePromise = Promise.promise();
            updatedItemsRecordFutures.add(updatePromise.future());
            JsonObject mappedItemAsJson = itemsJsonArray.getJsonObject(i);
            mappedItemAsJson = mappedItemAsJson.getJsonObject(ITEM_PATH_FIELD);
            LOGGER.debug(format("handle:: Updating Item with id: %s", mappedItemAsJson.getString("id")));
            List<String> validationErrors = validateItem(mappedItemAsJson, requiredFields);
            if (!validationErrors.isEmpty()) {
              String msg = format("Mapped Instance is invalid: %s, by jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' ", validationErrors,
                jobExecutionId, recordId, chunkId);
              LOGGER.warn("handle:: " + msg);
              dataImportEventPayload.getContext().put(ERRORS, Json.encode(validationErrors));
              errors.add(new PartialError(mappedItemAsJson.getString(ID_PATH_FIELD) != null ? mappedItemAsJson.getString(ID_PATH_FIELD) : BLANK, msg));
              updatePromise.complete();
            } else {
              String newItemStatus = mappedItemAsJson.getJsonObject(STATUS_KEY).getString("name");
              AtomicBoolean isProtectedStatusChanged = new AtomicBoolean();
              isProtectedStatusChanged.set(isProtectedStatusChanged(oldItemStatuses.get(mappedItemAsJson.getString(ID_PATH_FIELD)), newItemStatus));
              if (isProtectedStatusChanged.get()) {
                mappedItemAsJson.getJsonObject(STATUS_KEY).put("name", oldItemStatuses.get(mappedItemAsJson.getString(ID_PATH_FIELD)));
              }

              Item itemToUpdate = ItemUtil.jsonToItem(mappedItemAsJson);
              verifyItemBarcodeUniqueness(itemToUpdate, itemCollection, updatePromise, errors)
                .compose(v -> updateItemAndRetryIfOLExists(itemToUpdate, itemCollection, dataImportEventPayload, updatePromise, errors, expiredItems))
                .onSuccess(updatedItem -> {
                  if (isProtectedStatusChanged.get()) {
                    String msg = format(STATUS_UPDATE_ERROR_MSG, oldItemStatuses.get(updatedItem.getId()), newItemStatus);
                    LOGGER.warn("handle:: " + msg);
                    updatedItemEntities.add(updatedItem);
                    errors.add(new PartialError(updatedItem.getId() != null ? updatedItem.getId() : BLANK, msg));
                    updatePromise.complete();
                  } else {
                    addHoldingToPayloadIfNeeded(dataImportEventPayload, context, updatedItem)
                      .onComplete(item -> {
                        updatedItemEntities.add(updatedItem);
                        updatePromise.complete();
                      });
                  }
                });
            }
          }

          CompositeFuture.all(updatedItemsRecordFutures).onComplete(ar -> {
            if (!expiredItems.isEmpty()) {
              processOLError(dataImportEventPayload, future, itemCollection, expiredItems.get(0), errors);
            }
            if (dataImportEventPayload.getContext().containsKey(ERRORS) || !errors.isEmpty()) {
              dataImportEventPayload.getContext().put(ERRORS, Json.encode(errors));
            }
            if (ar.succeeded()) {
              List<JsonObject> itemsAsJsons = new ArrayList<>();
              for (Item updatedItemEntity : updatedItemEntities) {
                itemsAsJsons.add(ItemUtil.mapToJson(updatedItemEntity));
              }
              dataImportEventPayload.getContext().put(ITEM.value(), Json.encode(itemsAsJsons));
              future.complete(dataImportEventPayload);
            } else {
              future.completeExceptionally(ar.cause());
            }
            dataImportEventPayload.getContext().remove(TEMPORARY_MULTIPLE_HOLDINGS_FIELD);
          });
        })
        .onFailure(e -> {
          LOGGER.warn("handle:: Failed to update inventory Item by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' ", jobExecutionId,
            recordId, chunkId, e);
          future.completeExceptionally(e);
          dataImportEventPayload.getContext().remove(TEMPORARY_MULTIPLE_HOLDINGS_FIELD);
        });
    } catch (Exception e) {
      LOGGER.warn("handle:: Error updating inventory Item", e);
      future.completeExceptionally(e);
      dataImportEventPayload.getContext().remove(TEMPORARY_MULTIPLE_HOLDINGS_FIELD);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.ITEM;
    }
    return false;
  }

  private Map<String, String> preparePayloadAndGetStatus(DataImportEventPayload dataImportEventPayload) {
    Map<String, String> itemOldStatuses = new HashMap<>();

    JsonArray itemsJsonArray = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
    for (int i = 0; i < itemsJsonArray.size(); i++) {
      JsonObject itemAsJson = itemsJsonArray.getJsonObject(i);
      itemAsJson = itemAsJson.getJsonObject(ITEM_PATH_FIELD) != null ? itemAsJson.getJsonObject(ITEM_PATH_FIELD) : itemAsJson;
      itemOldStatuses.put(itemAsJson.getString(ID_PATH_FIELD), itemAsJson.getJsonObject(STATUS_KEY).getString("name"));
    }
    dataImportEventPayload.getContext().put(ITEM.value(), itemsJsonArray.encode());
    preparePayloadForMappingManager(dataImportEventPayload);
    return itemOldStatuses;
  }

  private Future<DataImportEventPayload> addHoldingToPayloadIfNeeded(DataImportEventPayload dataImportEventPayload, Context context, Item updatedItem) {
    Promise<DataImportEventPayload> promise = Promise.promise();
    HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
    JsonArray holdings = new JsonArray(dataImportEventPayload.getContext().get(HOLDINGS.value()) != null ? dataImportEventPayload.getContext().get(HOLDINGS.value()) : BLANK_JSON_ARRAY);
    if (holdings.stream().noneMatch(holding -> StringUtils.equals(((JsonObject) holding).getString("id"), updatedItem.getHoldingId()))) {
      holdingsRecordCollection.findById(updatedItem.getHoldingId(),
        success -> {
          LOGGER.info("addHoldingToPayloadIfNeeded:: Successfully retrieved Holdings for the hotlink by id: {}", updatedItem.getHoldingId());
          holdings.add(success.getResult());
          dataImportEventPayload.getContext().put(HOLDINGS.value(), holdings.encode());
          promise.complete(dataImportEventPayload);
        },
        failure -> {
          LOGGER.warn("addHoldingToPayloadIfNeeded:: Error retrieving Holdings for the hotlink by id {} cause {}, status code {}", updatedItem.getHoldingId(), failure.getReason(), failure.getStatusCode());
          promise.complete(dataImportEventPayload);
        });
    } else {
      LOGGER.debug("addHoldingToPayloadIfNeeded:: Holdings already exists in payload with for the hotlink with id {}", updatedItem.getHoldingId());
      promise.complete(dataImportEventPayload);
    }
    return promise.future();
  }

  private boolean isProtectedStatusChanged(String oldItemStatus, String newItemStatus) {
    return PROTECTED_STATUSES_FROM_UPDATE.contains(oldItemStatus) && !oldItemStatus.equals(newItemStatus);
  }

  private void preparePayloadForMappingManager(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getContext().put(CURRENT_EVENT_TYPE_PROPERTY, dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(CURRENT_NODE_PROPERTY, Json.encode(dataImportEventPayload.getCurrentNode()));

    JsonArray itemsJsonArray = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
    for (int i = 0; i < itemsJsonArray.size(); i++) {
      JsonObject itemAsJson = itemsJsonArray.getJsonObject(i);
      itemAsJson = itemAsJson.getJsonObject(ITEM_PATH_FIELD) != null ? itemAsJson.getJsonObject(ITEM_PATH_FIELD) : itemAsJson;
      itemsJsonArray.set(i, new JsonObject().put(ITEM_PATH_FIELD, itemAsJson));
    }
    dataImportEventPayload.getContext().put(ITEM.value(), itemsJsonArray.encode());
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private List<String> validateItem(JsonObject itemAsJson, List<String> requiredFields) {
    List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(itemAsJson, requiredFields);
    validateStatusName(itemAsJson, errors);
    return errors;
  }

  private void validateStatusName(JsonObject itemAsJson, List<String> errors) {
    String statusName = JsonHelper.getNestedProperty(itemAsJson, STATUS_KEY, "name");
    if (StringUtils.isNotBlank(statusName) && !ItemStatusName.isStatusCorrect(statusName)) {
      errors.add(format("Invalid status specified '%s'", statusName));
    }
  }

  private Future<Void> verifyItemBarcodeUniqueness(Item item, ItemCollection itemCollection, Promise<Void> updatePromise, List<PartialError> errors) {
    if (isEmpty(item.getBarcode())) {
      return Future.succeededFuture();
    }

    Promise<Void> promise = Promise.promise();
    try {
      itemCollection.findByCql(CqlHelper.barcodeIs(item.getBarcode()) + " AND id <> " + item.id, PagingParameters.defaults(),
        findResult -> {
          if (findResult.getResult().records.isEmpty()) {
            promise.complete();
          } else {
            LOGGER.warn("verifyItemBarcodeUniqueness:: Barcode must be unique, {} is already assigned to another item", item.getBarcode());
            errors.add(new PartialError(item.getId() != null ? item.getId() : BLANK, format("Barcode must be unique, %s is already assigned to another item", item.getBarcode())));
            updatePromise.complete();
            promise.fail(format("Barcode must be unique, %s is already assigned to another item", item.getBarcode()));
          }
        },
        failure -> {
          errors.add(new PartialError(item.getId() != null ? item.getId() : BLANK, failure.getReason()));
          updatePromise.complete();
          promise.fail(failure.getReason());
        });
    } catch (UnsupportedEncodingException e) {
      String msg = format("Failed to find items by barcode '%s'", item.getBarcode());
      LOGGER.warn("verifyItemBarcodeUniqueness:: " + msg, e);
      errors.add(new PartialError(item.getId() != null ? item.getId() : BLANK, format("Failed to find items by barcode '%s'", item.getBarcode())));
      updatePromise.complete();
      promise.fail(msg);
    }
    return promise.future();
  }

  private Future<Item> updateItemAndRetryIfOLExists(Item item, ItemCollection itemCollection, DataImportEventPayload eventPayload, Promise<Void> updatePromise, List<PartialError> errors, List<Item> expiredItems) {
    Promise<Item> promise = Promise.promise();
    item.getCirculationNotes().forEach(note -> note
      .withId(UUID.randomUUID().toString())
      .withSource(null)
      .withDate(dateTimeFormatter.format(ZonedDateTime.now())));

    itemCollection.update(item, success -> promise.complete(item),
      failure -> {
        errors.add(new PartialError(item.getId() != null ? item.getId() : BLANK, failure.getReason()));
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          expiredItems.add(item);
          updatePromise.complete();
          promise.fail(failure.getReason());
        } else {
          eventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          LOGGER.warn(format("updateItemAndRetryIfOLExists:: updating Item - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          updatePromise.complete();
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void processOLError(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, ItemCollection itemCollection, Item item, List<PartialError> errors) {
    int currentRetryNumber = dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER) == null ? 0 : Integer.parseInt(dataImportEventPayload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      dataImportEventPayload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("processOLError:: Error updating Item by id '{}'. Retry UpdateItemEventHandler handler...", item.getId());
      itemCollection.findById(item.getId())
        .thenAccept(actuaItem -> prepareDataAndReInvokeCurrentHandler(dataImportEventPayload, future, actuaItem))
        .thenAccept(v -> dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER))
        .exceptionally(e -> {
          dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Cannot get actual Item by id: '%s' for jobExecutionId '%s'. Error: %s ", item.getId(), dataImportEventPayload.getJobExecutionId(), e.getCause());
          LOGGER.warn("processOLError:: " + errMessage);
          errors.add(new PartialError(item.getId() != null ? item.getId() : BLANK, errMessage));
          future.complete(dataImportEventPayload);
          return null;
        });
    } else {
      dataImportEventPayload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded or equal given number %s for the Item update for jobExecutionId '%s' ", MAX_RETRIES_COUNT, currentRetryNumber, dataImportEventPayload.getJobExecutionId());
      LOGGER.warn("processOLError:: " + errMessage);
      errors.add(new PartialError(item.getId() != null ? item.getId() : BLANK, errMessage));
      future.complete(dataImportEventPayload);
    }
  }

  private void prepareDataAndReInvokeCurrentHandler(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, Item actualItem) {
    JsonArray resultedItems = updateActualItemAndConvertItemListAsJsonArray(dataImportEventPayload, actualItem);
    dataImportEventPayload.getContext().put(ITEM.value(), Json.encode(resultedItems));
    dataImportEventPayload.getEventsChain().remove(dataImportEventPayload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
    try {
      dataImportEventPayload.setCurrentNode(ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
    } catch (JsonProcessingException e) {
      LOGGER.warn("prepareDataAndReInvokeCurrentHandler:: Cannot map from CURRENT_NODE value", e);
    }
    dataImportEventPayload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
    dataImportEventPayload.getContext().remove(CURRENT_NODE_PROPERTY);
    dataImportEventPayload.getContext().put(MULTIPLE_HOLDINGS_FIELD, dataImportEventPayload.getContext().get(TEMPORARY_MULTIPLE_HOLDINGS_FIELD));
    dataImportEventPayload.getContext().remove(TEMPORARY_MULTIPLE_HOLDINGS_FIELD);
    handle(dataImportEventPayload).whenComplete((res, e) -> future.complete(dataImportEventPayload));
  }

  private static JsonArray updateActualItemAndConvertItemListAsJsonArray(DataImportEventPayload dataImportEventPayload, Item actualItem) {
    List<Item> initialItemList = new ArrayList<>();
    JsonArray itemsJsonArray = new JsonArray(dataImportEventPayload.getContext().get(ITEM.value()));
    for (int i = 0; i < itemsJsonArray.size(); i++) {
      Item currentItem = ItemUtil.jsonToItem(itemsJsonArray.getJsonObject(i).getJsonObject(ITEM_PATH_FIELD));
      initialItemList.add(currentItem);
    }
    List<Item> itemsList = new ArrayList<>(initialItemList);

    List<Item> updatedItemsList = new ArrayList<>(itemsList);
    for (int i = 0; i < itemsList.size(); i++) {
      Item item = itemsList.get(i);
      if (item.getId().equals(actualItem.getId())) {
        updatedItemsList.set(i, actualItem);
      }
    }

    JsonArray resultedItems = new JsonArray();
    for (Item currentItem : updatedItemsList) {
      resultedItems.add(new JsonObject().put(ITEM_PATH_FIELD, new JsonObject(ItemUtil.mapToMappingResultRepresentation(currentItem))));
    }
    return resultedItems;
  }
}
