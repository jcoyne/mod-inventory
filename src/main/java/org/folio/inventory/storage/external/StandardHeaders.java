package org.folio.inventory.storage.external;

import static org.apache.http.HttpHeaders.ACCEPT;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;

public class StandardHeaders {
  private static final String TENANT_HEADER = "X-Okapi-Tenant";
  private static final String TOKEN_HEADER = "X-Okapi-Token";
  private final String tenant;
  private final String token;

  public StandardHeaders(String tenant, String token) {
    this.tenant = tenant;
    this.token = token;
  }

  protected HttpRequest<Buffer> applyTo(HttpRequest<Buffer> request) {
    return request
      .putHeader(ACCEPT, "application/json, text/plain")
      .putHeader(TENANT_HEADER, tenant)
      .putHeader(TOKEN_HEADER, token);
  }
}
