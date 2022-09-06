package org.folio.inventory.storage.external;

import static org.folio.inventory.common.FutureAssistance.fail;
import static org.folio.inventory.common.FutureAssistance.getOnCompletion;
import static org.folio.inventory.common.FutureAssistance.succeed;
import static org.folio.inventory.common.FutureAssistance.waitForCompletion;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.folio.Authority;
import org.folio.inventory.common.WaitForAllFutures;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.junit.Test;

import lombok.SneakyThrows;

public class ExternalStorageModuleAuthorityRecordCollectionExamples extends ExternalStorageTests {
  private final ExternalStorageModuleAuthorityRecordCollection storage =
    useHttpClient(client -> new ExternalStorageModuleAuthorityRecordCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, client));

  @Test
  @SneakyThrows
  public void canBeEmptied() {
    addSomeExamples(storage);

    CompletableFuture<Void> emptied = new CompletableFuture<>();
    storage.empty(succeed(emptied), fail(emptied));
    waitForCompletion(emptied);
    CompletableFuture<MultipleRecords<Authority>> findFuture = new CompletableFuture<>();

    storage.findAll(PagingParameters.defaults(),
        succeed(findFuture), fail(findFuture));

    MultipleRecords<Authority> allInstancesWrapped = getOnCompletion(findFuture);

    List<Authority> allInstances = allInstancesWrapped.records;

    assertThat(allInstances.size(), is(0));
    assertThat(allInstancesWrapped.totalRecords, is(0));
  }

  @SneakyThrows
  private static void addSomeExamples(AuthorityRecordCollection authorityCollection) {
    WaitForAllFutures<Authority> allAdded = new WaitForAllFutures<>();
    authorityCollection.add(createAuthority(), allAdded.notifySuccess(), v -> {});
    authorityCollection.add(createAuthority(), allAdded.notifySuccess(), v -> {});
    allAdded.waitForCompletion();
  }

  private static Authority createAuthority() {
    return new Authority()
        .withId(UUID.randomUUID().toString());
  }
}
