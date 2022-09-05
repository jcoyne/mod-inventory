package org.folio.inventory.support;

import static org.folio.inventory.domain.converters.EntityConverters.converterForClass;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;
import static org.folio.inventory.support.JsonHelper.getNestedProperty;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.LastCheckIn;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public final class ItemUtil {
  private static final String MATERIAL_TYPE_ID_KEY = "materialTypeId";
  private static final String PERMANENT_LOAN_TYPE_ID_KEY = "permanentLoanTypeId";
  private static final String TEMPORARY_LOAN_TYPE_ID_KEY = "temporaryLoanTypeId";
  private static final String PERMANENT_LOCATION_ID_KEY = "permanentLocationId";
  private static final String TEMPORARY_LOCATION_ID_KEY = "temporaryLocationId";
  public static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  public static final String STATUS = "status";
  public static final String NUMBER_OF_PIECES = "numberOfPieces";
  public static final String COPY_NUMBER = "copyNumber";
  public static final String CHRONOLOGY = "chronology";
  public static final String ENUMERATION = "enumeration";
  public static final String BARCODE = "barcode";
  public static final String NOTES = "notes";
  public static final String ID = "id";
  public static final String MATERIAL_TYPE = "materialType";
  public static final String PERMANENT_LOCATION = "permanentLocation";
  public static final String TEMPORARY_LOCATION = "temporaryLocation";
  public static final String PERMANENT_LOAN_TYPE = "permanentLoanType";
  public static final String TEMPORARY_LOAN_TYPE = "temporaryLoanType";

  public static Item jsonToItem(JsonObject itemRequest) {
    List<String> formerIds = toListOfStrings(
      itemRequest.getJsonArray(Item.FORMER_IDS_KEY));

    List<String> statisticalCodeIds = toListOfStrings(
      itemRequest.getJsonArray(Item.STATISTICAL_CODE_IDS_KEY));

    List<String> yearCaption = toListOfStrings(
      itemRequest.getJsonArray(Item.YEAR_CAPTION_KEY));

    Status status = converterForClass(Status.class)
      .fromJson(itemRequest.getJsonObject(Item.STATUS_KEY));

    List<String> administrativeNotes = toListOfStrings(
      itemRequest.getJsonArray(Item.ADMINISTRATIVE_NOTES_KEY));

    List<Note> notes = itemRequest.containsKey(Item.NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.NOTES_KEY)).stream()
      .map(Note::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<CirculationNote> circulationNotes = itemRequest.containsKey(Item.CIRCULATION_NOTES_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.CIRCULATION_NOTES_KEY)).stream()
      .map(CirculationNote::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<ElectronicAccess> electronicAccess = itemRequest.containsKey(Item.ELECTRONIC_ACCESS_KEY)
      ? JsonArrayHelper.toList(itemRequest.getJsonArray(Item.ELECTRONIC_ACCESS_KEY)).stream()
      .map(ElectronicAccess::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    List<String> tags = itemRequest.containsKey(Item.TAGS_KEY)
      ? getTags(itemRequest) : new ArrayList<>();


    String materialTypeId = getNestedProperty(itemRequest, MATERIAL_TYPE, ID);
    String permanentLocationId = getNestedProperty(itemRequest, PERMANENT_LOCATION, ID);
    String temporaryLocationId = getNestedProperty(itemRequest, TEMPORARY_LOCATION, ID);
    String permanentLoanTypeId = getNestedProperty(itemRequest, PERMANENT_LOAN_TYPE, ID);
    String temporaryLoanTypeId = getNestedProperty(itemRequest, TEMPORARY_LOAN_TYPE, ID);

    return new Item(
      itemRequest.getString(ID),
      itemRequest.getString(Item.VERSION_KEY),
      itemRequest.getString(HOLDINGS_RECORD_ID),
      itemRequest.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      status,
      materialTypeId,
      permanentLoanTypeId,
      null)
      .withHrid(itemRequest.getString(Item.HRID_KEY))
      .withFormerIds(formerIds)
      .withDiscoverySuppress(itemRequest.getBoolean(Item.DISCOVERY_SUPPRESS_KEY))
      .withBarcode(itemRequest.getString(BARCODE))
      .withItemLevelCallNumber(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_KEY))
      .withEffectiveShelvingOrder(itemRequest.getString(Item.EFFECTIVE_SHELVING_ORDER_KEY))
      .withItemLevelCallNumberPrefix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY))
      .withItemLevelCallNumberSuffix(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY))
      .withItemLevelCallNumberTypeId(itemRequest.getString(Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY))
      .withVolume(itemRequest.getString(Item.VOLUME_KEY))
      .withEnumeration(itemRequest.getString(ENUMERATION))
      .withChronology(itemRequest.getString(CHRONOLOGY))
      .withNumberOfPieces(itemRequest.getString(NUMBER_OF_PIECES))
      .withDescriptionOfPieces(itemRequest.getString(Item.DESCRIPTION_OF_PIECES_KEY))
      .withNumberOfMissingPieces(itemRequest.getString(Item.NUMBER_OF_MISSING_PIECES_KEY))
      .withMissingPieces(itemRequest.getString(Item.MISSING_PIECES_KEY))
      .withMissingPiecesDate(itemRequest.getString(Item.MISSING_PIECES_DATE_KEY))
      .withItemDamagedStatusId(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_ID_KEY))
      .withItemDamagedStatusDate(itemRequest.getString(Item.ITEM_DAMAGED_STATUS_DATE_KEY))
      .withPermanentLocationId(permanentLocationId)
      .withTemporaryLocationId(temporaryLocationId)
      .withTemporaryLoanTypeId(temporaryLoanTypeId)
      .withCopyNumber(itemRequest.getString(Item.COPY_NUMBER_KEY))
      .withAdministrativeNotes(administrativeNotes)
      .withNotes(notes)
      .withCirculationNotes(circulationNotes)
      .withAccessionNumber(itemRequest.getString(Item.ACCESSION_NUMBER_KEY))
      .withItemIdentifier(itemRequest.getString(Item.ITEM_IDENTIFIER_KEY))
      .withYearCaption(yearCaption)
      .withElectronicAccess(electronicAccess)
      .withStatisticalCodeIds(statisticalCodeIds)
      .withPurchaseOrderLineIdentifier(itemRequest.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER))
      .withLastCheckIn(LastCheckIn.from(itemRequest.getJsonObject(Item.LAST_CHECK_IN)))
      .withTags(tags);
  }

  private static List<String> getTags(JsonObject itemRequest) {
    final JsonObject tags = itemRequest.getJsonObject(Item.TAGS_KEY);
    return tags.containsKey(Item.TAG_LIST_KEY) ?
      JsonArrayHelper.toListOfStrings(tags.getJsonArray(Item.TAG_LIST_KEY)) : new ArrayList<>();
  }

  public static JsonObject mapToJson(Item item) {
    JsonObject itemJson = new JsonObject();
    itemJson.put(ID, item.id != null
      ? item.id
      : UUID.randomUUID().toString());

    includeIfPresent(itemJson, Item.VERSION_KEY, item.getVersion());
    itemJson.put(STATUS, converterForClass(Status.class).toJson(item.getStatus()));

    if(item.getLastCheckIn() != null) {
      itemJson.put(Item.LAST_CHECK_IN, item.getLastCheckIn().toJson());
    }

    includeIfPresent(itemJson, Item.HRID_KEY, item.getHrid());
    includeIfPresent(itemJson, Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY,
      item.getInTransitDestinationServicePointId());
    itemJson.put(Item.FORMER_IDS_KEY, item.getFormerIds());
    itemJson.put(Item.DISCOVERY_SUPPRESS_KEY, item.getDiscoverySuppress());
    includeIfPresent(itemJson, COPY_NUMBER, item.getCopyNumber());
    itemJson.put(Item.ADMINISTRATIVE_NOTES_KEY, item.getAdministrativeNotes());
    itemJson.put(NOTES, item.getNotes());
    itemJson.put(Item.CIRCULATION_NOTES_KEY, item.getCirculationNotes());
    includeIfPresent(itemJson, BARCODE, item.getBarcode());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_KEY, item.getItemLevelCallNumber());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY, item.getItemLevelCallNumberPrefix());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY, item.getItemLevelCallNumberSuffix());
    includeIfPresent(itemJson, Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY, item.getItemLevelCallNumberTypeId());
    includeIfPresent(itemJson, Item.VOLUME_KEY, item.getVolume());
    includeIfPresent(itemJson, ENUMERATION, item.getEnumeration());
    includeIfPresent(itemJson, CHRONOLOGY, item.getChronology());
    includeIfPresent(itemJson, NUMBER_OF_PIECES, item.getNumberOfPieces());
    includeIfPresent(itemJson, Item.DESCRIPTION_OF_PIECES_KEY, item.getDescriptionOfPieces());
    includeIfPresent(itemJson, Item.NUMBER_OF_MISSING_PIECES_KEY, item.getNumberOfMissingPieces());
    includeIfPresent(itemJson, Item.MISSING_PIECES_KEY, item.getMissingPieces());
    includeIfPresent(itemJson, Item.MISSING_PIECES_DATE_KEY, item.getMissingPiecesDate());
    includeIfPresent(itemJson, Item.ITEM_DAMAGED_STATUS_ID_KEY, item.getItemDamagedStatusId());
    includeIfPresent(itemJson, Item.ITEM_DAMAGED_STATUS_DATE_KEY, item.getItemDamagedStatusDate());
    includeIfPresent(itemJson, HOLDINGS_RECORD_ID, item.getHoldingId());
    includeIfPresent(itemJson, MATERIAL_TYPE_ID_KEY, item.getMaterialTypeId());
    includeIfPresent(itemJson, PERMANENT_LOAN_TYPE_ID_KEY, item.getPermanentLoanTypeId());
    includeIfPresent(itemJson, TEMPORARY_LOAN_TYPE_ID_KEY, item.getTemporaryLoanTypeId());
    includeIfPresent(itemJson, PERMANENT_LOCATION_ID_KEY, item.getPermanentLocationId());
    includeIfPresent(itemJson, TEMPORARY_LOCATION_ID_KEY, item.getTemporaryLocationId());
    includeIfPresent(itemJson, Item.ACCESSION_NUMBER_KEY, item.getAccessionNumber());
    includeIfPresent(itemJson, Item.ITEM_IDENTIFIER_KEY, item.getItemIdentifier());
    itemJson.put(Item.YEAR_CAPTION_KEY, item.getYearCaption());
    itemJson.put(Item.ELECTRONIC_ACCESS_KEY, item.getElectronicAccess());
    itemJson.put(Item.STATISTICAL_CODE_IDS_KEY, item.getStatisticalCodeIds());
    itemJson.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, item.getPurchaseOrderLineIdentifier());
    itemJson.put(Item.TAGS_KEY, new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray(item.getTags())));

    return itemJson;
  }

  public static String mapToMappingResultRepresentation(Item item) {
    JsonObject itemJson = mapToJson(item);

    if (itemJson.getString(MATERIAL_TYPE_ID_KEY) != null) {
      itemJson.put(MATERIAL_TYPE, new JsonObject().put(ID, itemJson.remove(MATERIAL_TYPE_ID_KEY)));
    }
    if (itemJson.getString(PERMANENT_LOAN_TYPE_ID_KEY) != null) {
      itemJson.put(PERMANENT_LOAN_TYPE, new JsonObject().put(ID, itemJson.remove(PERMANENT_LOAN_TYPE_ID_KEY)));
    }
    if (itemJson.getString(TEMPORARY_LOAN_TYPE_ID_KEY) != null) {
      itemJson.put(TEMPORARY_LOAN_TYPE, new JsonObject().put(ID, itemJson.remove(TEMPORARY_LOAN_TYPE_ID_KEY)));
    }
    if (itemJson.getString(PERMANENT_LOCATION_ID_KEY) != null) {
      itemJson.put(PERMANENT_LOCATION, new JsonObject().put(ID, itemJson.remove(PERMANENT_LOCATION_ID_KEY)));
    }
    if (itemJson.getString(TEMPORARY_LOCATION_ID_KEY) != null) {
      itemJson.put(TEMPORARY_LOCATION, new JsonObject().put(ID, itemJson.remove(TEMPORARY_LOCATION_ID_KEY)));
    }

    return itemJson.encode();
  }
}
