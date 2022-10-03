package org.folio.inventory.storage;

import java.util.function.Function;

import org.folio.inventory.common.Context;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;
import org.folio.inventory.storage.external.ExternalStorageCollections;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

public class Storage {
  private final Function<Context, ExternalStorageCollections> collectionFactory;

  private Storage(final Function<Context, ExternalStorageCollections> collectionFactory) {
    this.collectionFactory = collectionFactory;
  }

  public static Storage basedUpon(JsonObject config, HttpClient client) {
    String storageType = config.getString("storage.type", "okapi");

    switch(storageType) {
      case "external":
        return useExternalLocation(client,
          config.getString("storage.location", null));

      case "okapi":
        return useOkapi(client);

      default:
        throw new IllegalArgumentException("Storage type must be one of [external, okapi]");
    }
  }

  private static Storage useExternalLocation(HttpClient client, String location) {
    if (location == null) {
      throw new IllegalArgumentException(
        "For external storage, location must be provided.");
    }

    return new Storage(context -> new ExternalStorageCollections(location, client));
  }

  public static Storage useOkapi(HttpClient client) {
    return new Storage(context ->
      new ExternalStorageCollections(context.getOkapiLocation(), client));
  }

  public ItemCollection getItemCollection(Context context) {
    return collectionFactory.apply(context).getItemCollection(
      context.getTenantId(), context.getToken());
  }

  public InstanceCollection getInstanceCollection(Context context) {
    return collectionFactory.apply(context).getInstanceCollection(
      context.getTenantId(), context.getToken());
  }

  public HoldingsRecordCollection getHoldingsRecordCollection(Context context) {
    return collectionFactory.apply(context).getHoldingsRecordCollection(
      context.getTenantId(), context.getToken());
  }

  public HoldingsRecordsSourceCollection getHoldingsRecordsSourceCollection (Context context){
    return collectionFactory.apply(context).getHoldingsRecordsSourceCollection(
      context.getTenantId(), context.getToken()
    );
  }

  public AuthorityRecordCollection getAuthorityRecordCollection(Context context) {
    return collectionFactory.apply(context).getAuthorityCollection(
      context.getTenantId(), context.getToken());
  }

  public UserCollection getUserCollection(Context context) {
    return collectionFactory.apply(context).getUserCollection(
      context.getTenantId(), context.getToken());
  }
}
