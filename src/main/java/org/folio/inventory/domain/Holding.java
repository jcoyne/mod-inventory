package org.folio.inventory.domain;

import lombok.Getter;

public class Holding {
  @Getter
  public final String id;
  public final String instanceId;
  public final String permanentLocationId;

  public Holding(String id, String instanceId, String permanentLocationId) {
    this.id = id;
    this.instanceId = instanceId;
    this.permanentLocationId = permanentLocationId;
  }
}
