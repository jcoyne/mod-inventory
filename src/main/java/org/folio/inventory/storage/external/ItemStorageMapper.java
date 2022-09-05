package org.folio.inventory.storage.external;

import static org.folio.inventory.domain.converters.EntityConverters.converterForClass;
import static org.folio.inventory.support.JsonArrayHelper.toList;
import static org.folio.inventory.support.JsonHelper.includeIfPresent;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.EffectiveCallNumberComponents;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.LastCheckIn;
import org.folio.inventory.domain.items.Note;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.JsonArrayHelper;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ItemStorageMapper implements StorageMapper<Item> {

  private static final String MATERIAL_TYPE_ID_KEY = "materialTypeId";
  private static final String HOLDINGS_RECORD_ID = "holdingsRecordId";
  private static final String PERMANENT_LOAN_TYPE_ID_KEY = "permanentLoanTypeId";
  private static final String TEMPORARY_LOAN_TYPE_ID_KEY = "temporaryLoanTypeId";
  private static final String PERMANENT_LOCATION_ID_KEY = "permanentLocationId";
  private static final String TEMPORARY_LOCATION_ID_KEY = "temporaryLocationId";

  public JsonObject mapToRequest(Item item) {
    JsonObject itemToSend = new JsonObject();

    //TODO: Review if this shouldn't be defaulting here
    itemToSend.put(ItemUtil.ID, item.id != null
      ? item.id
      : UUID.randomUUID().toString());

    includeIfPresent(itemToSend, Item.VERSION_KEY, item.getVersion());

    itemToSend.put(ItemUtil.STATUS, converterForClass(Status.class).toJson(item.getStatus()));

    if (item.getLastCheckIn() != null) {
      itemToSend.put(Item.LAST_CHECK_IN, item.getLastCheckIn().toJson());
    }
    includeIfPresent(itemToSend, Item.HRID_KEY, item.getHrid());
    includeIfPresent(itemToSend, Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY,
      item.getInTransitDestinationServicePointId());
    itemToSend.put(Item.FORMER_IDS_KEY, item.getFormerIds());
    itemToSend.put(Item.DISCOVERY_SUPPRESS_KEY, item.getDiscoverySuppress());
    includeIfPresent(itemToSend, ItemUtil.COPY_NUMBER, item.getCopyNumber());
    itemToSend.put(Item.ADMINISTRATIVE_NOTES_KEY, item.getAdministrativeNotes());
    itemToSend.put(ItemUtil.NOTES, item.getNotes());
    itemToSend.put(Item.CIRCULATION_NOTES_KEY, item.getCirculationNotes());
    includeIfPresent(itemToSend, ItemUtil.BARCODE, item.getBarcode());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_KEY, item.getItemLevelCallNumber());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY, item.getItemLevelCallNumberPrefix());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY, item.getItemLevelCallNumberSuffix());
    includeIfPresent(itemToSend, Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY, item.getItemLevelCallNumberTypeId());
    includeIfPresent(itemToSend, Item.VOLUME_KEY, item.getVolume());
    includeIfPresent(itemToSend, ItemUtil.ENUMERATION, item.getEnumeration());
    includeIfPresent(itemToSend, ItemUtil.CHRONOLOGY, item.getChronology());
    includeIfPresent(itemToSend, ItemUtil.NUMBER_OF_PIECES, item.getNumberOfPieces());
    includeIfPresent(itemToSend, Item.DESCRIPTION_OF_PIECES_KEY, item.getDescriptionOfPieces());
    includeIfPresent(itemToSend, Item.NUMBER_OF_MISSING_PIECES_KEY, item.getNumberOfMissingPieces());
    includeIfPresent(itemToSend, Item.MISSING_PIECES_KEY, item.getMissingPieces());
    includeIfPresent(itemToSend, Item.MISSING_PIECES_DATE_KEY, item.getMissingPiecesDate());
    includeIfPresent(itemToSend, Item.ITEM_DAMAGED_STATUS_ID_KEY, item.getItemDamagedStatusId());
    includeIfPresent(itemToSend, Item.ITEM_DAMAGED_STATUS_DATE_KEY, item.getItemDamagedStatusDate());
    includeIfPresent(itemToSend, HOLDINGS_RECORD_ID, item.getHoldingId());
    includeIfPresent(itemToSend, MATERIAL_TYPE_ID_KEY, item.getMaterialTypeId());
    includeIfPresent(itemToSend, PERMANENT_LOAN_TYPE_ID_KEY, item.getPermanentLoanTypeId());
    includeIfPresent(itemToSend, TEMPORARY_LOAN_TYPE_ID_KEY, item.getTemporaryLoanTypeId());
    includeIfPresent(itemToSend, PERMANENT_LOCATION_ID_KEY, item.getPermanentLocationId());
    includeIfPresent(itemToSend, TEMPORARY_LOCATION_ID_KEY, item.getTemporaryLocationId());
    includeIfPresent(itemToSend, Item.ACCESSION_NUMBER_KEY, item.getAccessionNumber());
    includeIfPresent(itemToSend, Item.ITEM_IDENTIFIER_KEY, item.getItemIdentifier());
    itemToSend.put(Item.YEAR_CAPTION_KEY, item.getYearCaption());
    itemToSend.put(Item.ELECTRONIC_ACCESS_KEY, item.getElectronicAccess());
    itemToSend.put(Item.STATISTICAL_CODE_IDS_KEY, item.getStatisticalCodeIds());
    itemToSend.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, item.getPurchaseOrderLineIdentifier());
    itemToSend.put(Item.TAGS_KEY, new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray(item.getTags())));

    return itemToSend;
  }

  public Item mapFromResponse(JsonObject itemFromServer) {
    List<String> formerIds = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.FORMER_IDS_KEY));
    List<String> statisticalCodeIds = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.STATISTICAL_CODE_IDS_KEY));
    List<String> yearCaption = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.YEAR_CAPTION_KEY));

    List<JsonObject> notes = toList(
      itemFromServer.getJsonArray(Item.NOTES_KEY, new JsonArray()));

    List<Note> mappedNotes = notes.stream()
      .map(Note::new)
      .collect(Collectors.toList());

    List<JsonObject> circulationNotes = toList(
      itemFromServer.getJsonArray(Item.CIRCULATION_NOTES_KEY, new JsonArray()));

    List<String> administrativeNotes = JsonArrayHelper
      .toListOfStrings(itemFromServer.getJsonArray(Item.ADMINISTRATIVE_NOTES_KEY));

    List<CirculationNote> mappedCirculationNotes = circulationNotes.stream()
      .map(CirculationNote::new)
      .collect(Collectors.toList());

    List<JsonObject> electronicAccess = toList(
      itemFromServer.getJsonArray(Item.ELECTRONIC_ACCESS_KEY, new JsonArray()));

    List<ElectronicAccess> mappedElectronicAccess = electronicAccess.stream()
      .map(ElectronicAccess::new)
      .collect(Collectors.toList());

    List<String> tags = itemFromServer.containsKey(Item.TAGS_KEY)
      ? JsonArrayHelper.toListOfStrings(
      itemFromServer.getJsonObject(Item.TAGS_KEY).getJsonArray(Item.TAG_LIST_KEY))
      : new ArrayList<>();

    return new Item(
      itemFromServer.getString(ItemUtil.ID),
      itemFromServer.getString(Item.VERSION_KEY),
      itemFromServer.getString(ItemUtil.HOLDINGS_RECORD_ID),
      itemFromServer.getString(Item.TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY),
      converterForClass(Status.class).fromJson(itemFromServer.getJsonObject(
        ItemUtil.STATUS)),
      itemFromServer.getString(MATERIAL_TYPE_ID_KEY),
      itemFromServer.getString(PERMANENT_LOAN_TYPE_ID_KEY),
      itemFromServer.getJsonObject("metadata"))
      .withHrid(itemFromServer.getString(Item.HRID_KEY))
      .withEffectiveShelvingOrder(itemFromServer.getString(Item.EFFECTIVE_SHELVING_ORDER_KEY))
      .withFormerIds(formerIds)
      .withDiscoverySuppress(itemFromServer.getBoolean(Item.DISCOVERY_SUPPRESS_KEY))
      .withBarcode(itemFromServer.getString(ItemUtil.BARCODE))
      .withItemLevelCallNumber(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_KEY))
      .withItemLevelCallNumberPrefix(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY))
      .withItemLevelCallNumberSuffix(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY))
      .withItemLevelCallNumberTypeId(itemFromServer.getString(Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY))
      .withVolume(itemFromServer.getString(Item.VOLUME_KEY))
      .withEnumeration(itemFromServer.getString(ItemUtil.ENUMERATION))
      .withChronology(itemFromServer.getString(ItemUtil.CHRONOLOGY))
      .withCopyNumber(itemFromServer.getString(ItemUtil.COPY_NUMBER))
      .withNumberOfPieces(itemFromServer.getString(ItemUtil.NUMBER_OF_PIECES))
      .withDescriptionOfPieces(itemFromServer.getString(Item.DESCRIPTION_OF_PIECES_KEY))
      .withNumberOfMissingPieces(itemFromServer.getString(Item.NUMBER_OF_MISSING_PIECES_KEY))
      .withMissingPieces(itemFromServer.getString(Item.MISSING_PIECES_KEY))
      .withMissingPiecesDate(itemFromServer.getString(Item.MISSING_PIECES_DATE_KEY))
      .withItemDamagedStatusId(itemFromServer.getString(Item.ITEM_DAMAGED_STATUS_ID_KEY))
      .withItemDamagedStatusDate(itemFromServer.getString(Item.ITEM_DAMAGED_STATUS_DATE_KEY))
      .withAdministrativeNotes(administrativeNotes)
      .withNotes(mappedNotes)
      .withCirculationNotes(mappedCirculationNotes)
      .withPermanentLocationId(itemFromServer.getString(PERMANENT_LOCATION_ID_KEY))
      .withTemporaryLocationId(itemFromServer.getString(TEMPORARY_LOCATION_ID_KEY))
      .withEffectiveLocationId(itemFromServer.getString("effectiveLocationId"))
      .withTemporaryLoanTypeId(itemFromServer.getString(TEMPORARY_LOAN_TYPE_ID_KEY))
      .withAccessionNumber(itemFromServer.getString(Item.ACCESSION_NUMBER_KEY))
      .withItemIdentifier(itemFromServer.getString(Item.ITEM_IDENTIFIER_KEY))
      .withYearCaption(yearCaption)
      .withElectronicAccess(mappedElectronicAccess)
      .withStatisticalCodeIds(statisticalCodeIds)
      .withPurchaseOrderLineIdentifier(itemFromServer.getString(Item.PURCHASE_ORDER_LINE_IDENTIFIER))
      .withTags(tags)
      .withLastCheckIn(LastCheckIn.from(itemFromServer.getJsonObject("lastCheckIn")))
      .withEffectiveCallNumberComponents(
        EffectiveCallNumberComponents.from(itemFromServer.getJsonObject("effectiveCallNumberComponents")));
  }
}
