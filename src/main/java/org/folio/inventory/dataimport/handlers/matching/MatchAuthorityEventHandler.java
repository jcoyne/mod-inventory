package org.folio.inventory.dataimport.handlers.matching;

import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_NOT_MATCHED;

public class MatchAuthorityEventHandler extends AbstractMatchEventHandler {

  public MatchAuthorityEventHandler(MappingMetadataCache mappingMetadataCache) {
    super(mappingMetadataCache);
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.AUTHORITY;
  }

  @Override
  protected String getMatchedEventType() {
    return DI_INVENTORY_AUTHORITY_MATCHED.value();
  }

  @Override
  protected String getNotMatchedEventType() {
    return DI_INVENTORY_AUTHORITY_NOT_MATCHED.value();
  }
}
