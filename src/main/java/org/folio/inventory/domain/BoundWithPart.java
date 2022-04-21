package org.folio.inventory.domain;

import lombok.Value;

@Value
public class BoundWithPart {
  String itemId;
  String holdingsRecordId;
}
