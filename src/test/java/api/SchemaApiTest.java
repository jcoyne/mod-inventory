package api;

import api.support.ApiRoot;
import api.support.ApiTests;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SchemaApiTest extends ApiTests {

  @Test
  public void shouldCreateSchemaWithTables() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var postCompleted = okapiClient
      .post(ApiRoot.tenant(), "{}");

    Response postResponse = postCompleted.toCompletableFuture().get(10, SECONDS);
    assertThat(postResponse.getStatusCode(), is(200));
  }

}