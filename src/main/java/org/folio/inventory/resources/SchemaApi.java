package org.folio.inventory.resources;

import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.inventory.common.WebContext;
import java.sql.Connection;

import static java.lang.String.format;
import static org.folio.inventory.rest.util.ModuleUtil.convertToPsqlStandard;
import static org.folio.inventory.rest.impl.SingleConnectionProvider.getConnection;


public class SchemaApi {

  private static final Logger LOGGER = LogManager.getLogger(SchemaApi.class);

  private static final String CHANGELOG_TENANT_PATH = "liquibase/tenant/changelog.xml";
  private static final String TENANT_PATH = "/_/tenant";

  public void register(Router router) {
    router.post(TENANT_PATH).handler(this::create);
  }

  public void create(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    initializeSchemaForTenant(context.getTenantId())
      .onSuccess(result -> routingContext.response().setStatusCode(HttpStatus.HTTP_OK.toInt()).end())
      .onFailure(fail -> routingContext.response().setStatusCode(HttpStatus.HTTP_INTERNAL_SERVER_ERROR.toInt()).end(fail.toString()));
  }

  public static Future<Integer> initializeSchemaForTenant(String tenant) {
    String schemaName = convertToPsqlStandard(tenant);
    LOGGER.info("Initializing schema {} for tenant {}", schemaName, tenant);
    try (Connection connection = getConnection(tenant)) {
      boolean schemaIsNotExecuted = connection.prepareStatement(format("CREATE SCHEMA IF NOT EXISTS %s", schemaName)).execute();
      if (schemaIsNotExecuted) {
        return Future.failedFuture(String.format("Cannot create schema %s", schemaName));
      } else {
        LOGGER.info("Schema {} created or already exists", schemaName);
        runScripts(schemaName, connection);
        LOGGER.info("Schema is initialized for tenant {}", tenant);
        return Future.succeededFuture(0);
      }
    } catch (Exception e) {
      String cause = format("Error while initializing schema %s for tenant %s", schemaName, tenant);
      LOGGER.error(cause, e);
      return Future.failedFuture(cause);
    }
  }

  private static void runScripts(String schemaName, Connection connection) throws LiquibaseException {
    Liquibase liquibase = null;
    try {
      Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
      database.setDefaultSchemaName(schemaName);
      liquibase = new Liquibase(CHANGELOG_TENANT_PATH, new ClassLoaderResourceAccessor(), database);
      liquibase.update(new Contexts());
    } finally {
      if (liquibase != null && liquibase.getDatabase() != null) {
        Database database = liquibase.getDatabase();
        database.close();
      }
    }
  }
}