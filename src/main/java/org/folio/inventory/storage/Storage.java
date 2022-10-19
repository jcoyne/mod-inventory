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

public class Storage {
  private final Function<Context, ExternalStorageCollections> collectionFactory;

  private Storage(final Function<Context, ExternalStorageCollections> collectionFactory) {
    this.collectionFactory = collectionFactory;
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
