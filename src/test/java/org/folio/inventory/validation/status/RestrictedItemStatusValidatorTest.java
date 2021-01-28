package org.folio.inventory.validation.status;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.SneakyThrows;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.domain.items.Status;
import org.folio.inventory.validation.status.TargetItemStatusValidators;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.inventory.domain.items.ItemStatusName.RESTRICTED;
import static org.junit.Assert.assertThrows;

@RunWith(JUnitParamsRunner.class)
public class RestrictedItemStatusValidatorTest {
  private static TargetItemStatusValidators validator;

  @BeforeClass
  public static void setUp() throws Exception {
    validator = new TargetItemStatusValidators();
  }

  @SneakyThrows
  @Parameters({
    "Available",
    "Awaiting pickup",
    "Awaiting delivery",
    "In transit",
    "Lost and paid",
    "Missing",
    "Order closed",
    "Paged",
    "Withdrawn"
  })
  @Test
  public void itemCanBeMarkedAsRestrictedWhenInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(RESTRICTED);

    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);

    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    validationFuture.get(1, TimeUnit.SECONDS);

    // Validator responds with a successful future when valid
    assertThat(validationFuture.isDone()).isTrue();
  }

  @Parameters({
    "Aged to lost",
    "Checked out",
    "Claimed returned",
    "Declared lost",
    "In process",
    "In process (non-requestable)",
    "Intellectual item",
    "Long missing",
    "On order",
    "Restricted",
    "Unavailable",
    "Unknown"
  })
  @Test
  public void itemCannotBeMarkedAsRestrictedWhenNotInAcceptableSourceStatus(String sourceStatus) {
    final var targetValidator = validator.getValidator(RESTRICTED);
    final var item = new Item(null, null, new Status(ItemStatusName.forName(sourceStatus)), null, null, null);
    final var validationFuture = targetValidator.refuseItemWhenNotInAcceptableSourceStatus(item);

    Exception e = assertThrows(
      Exception.class,() -> validationFuture.get(1,TimeUnit.SECONDS)
    );

    assertThat(e.getCause().getMessage()).isEqualTo("Item is not allowed to be marked as Restricted");
  }
}