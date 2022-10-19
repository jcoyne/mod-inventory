package org.folio.inventory;

import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_MAX_REQUEST_SIZE;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.folio.inventory.common.VertxAssistant;

public class Launcher {
  private static final String DATA_IMPORT_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.DataImportConsumerVerticle.instancesNumber";
  private static final String MARC_BIB_INSTANCE_HRID_SET_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.MarcBibInstanceHridSetConsumerVerticle.instancesNumber";
  private static final String QUICK_MARC_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG = "inventory.kafka.QuickMarcConsumerVerticle.instancesNumber";
  private static final VertxAssistant vertxAssistant = new VertxAssistant();

  private static String inventoryModuleDeploymentId;
  private static String consumerVerticleDeploymentId;
  private static String marcInstHridSetConsumerVerticleDeploymentId;
  private static String quickMarcConsumerVerticleDeploymentId;

  public static void main(String[] args)
    throws InterruptedException, ExecutionException, TimeoutException {

    Logging.initialiseFormat();

    Runtime.getRuntime().addShutdownHook(new Thread(Launcher::stop));

    final var config = new HashMap<String, Object>();

    final var portString = System.getProperty("http.port", System.getProperty("port", "9403"));
    final var port = Integer.valueOf(portString);

    putNonNullConfig("port", port, config);

    final var deployKafkaConsumerVerticles = System.getProperty(
      "org.folio.metadata.inventory.kafka.consumers.initialized", "true");

    start(config);

    if (Boolean.parseBoolean(deployKafkaConsumerVerticles)) {
      startConsumerVerticles(getConsumerVerticleConfig());
    }
  }

  private static void start(Map<String, Object> config)
    throws InterruptedException, ExecutionException, TimeoutException {

    final var log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    vertxAssistant.start();

    log.info("Server Starting");

    final var deployed = new CompletableFuture<String>();

    vertxAssistant.deployVerticle(InventoryVerticle.class.getName(),
      config, deployed);

    deployed.thenAccept(v -> log.info("Server Started"));

    inventoryModuleDeploymentId = deployed.get(20, TimeUnit.SECONDS);
  }

  private static void startConsumerVerticles(Map<String, Object> consumerVerticlesConfig)
    throws InterruptedException, ExecutionException, TimeoutException {

    final var dataImportConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(DATA_IMPORT_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    final var instanceHridSetConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(MARC_BIB_INSTANCE_HRID_SET_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "3"));
    final var quickMarcConsumerVerticleNumber = Integer.parseInt(System.getenv().getOrDefault(QUICK_MARC_CONSUMER_VERTICLE_INSTANCES_NUMBER_CONFIG, "1"));

    final var dataImportVerticleDeployed = new CompletableFuture<String>();
    final var marcInstanceHridVerticleDeployed = new CompletableFuture<String>();
    final var quickMarcVerticleDeployed = new CompletableFuture<String>();

    vertxAssistant.deployVerticle(DataImportConsumerVerticle.class.getName(),
      consumerVerticlesConfig, dataImportConsumerVerticleNumber, dataImportVerticleDeployed);

    vertxAssistant.deployVerticle(MarcHridSetConsumerVerticle.class.getName(),
      consumerVerticlesConfig, instanceHridSetConsumerVerticleNumber, marcInstanceHridVerticleDeployed);

    vertxAssistant.deployVerticle(QuickMarcConsumerVerticle.class.getName(),
      consumerVerticlesConfig, quickMarcConsumerVerticleNumber, quickMarcVerticleDeployed);

    consumerVerticleDeploymentId = dataImportVerticleDeployed.get(20, TimeUnit.SECONDS);
    marcInstHridSetConsumerVerticleDeploymentId = marcInstanceHridVerticleDeployed.get(20, TimeUnit.SECONDS);
    quickMarcConsumerVerticleDeploymentId = quickMarcVerticleDeployed.get(20, TimeUnit.SECONDS);
  }

  private static void stop() {
    final var log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    final var stopped = new CompletableFuture<Void>();

    log.info("Server Stopping");

    vertxAssistant.undeployVerticle(inventoryModuleDeploymentId)
      .thenCompose(v -> vertxAssistant.undeployVerticle(consumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(marcInstHridSetConsumerVerticleDeploymentId))
      .thenCompose(v -> vertxAssistant.undeployVerticle(quickMarcConsumerVerticleDeploymentId))
      .thenAccept(v -> vertxAssistant.stop(stopped));

    stopped.thenAccept(v -> log.info("Server Stopped"));
  }

  private static void putNonNullConfig(String key, Object value, Map<String, Object> config) {
    if (value != null) {
      config.put(key, value);
    }
  }

  private static Map<String, Object> getConsumerVerticleConfig() {
    final var config = new HashMap<String, Object>();

    config.put(KAFKA_HOST, System.getenv().getOrDefault(KAFKA_HOST, "kafka"));
    config.put(KAFKA_PORT, System.getenv().getOrDefault(KAFKA_PORT, "9092"));
    config.put(OKAPI_URL, System.getenv().getOrDefault(OKAPI_URL, "http://okapi:9130"));
    config.put(KAFKA_REPLICATION_FACTOR, System.getenv().getOrDefault(KAFKA_REPLICATION_FACTOR, "1"));
    config.put(KAFKA_ENV, System.getenv().getOrDefault(KAFKA_ENV, "folio"));
    config.put(KAFKA_MAX_REQUEST_SIZE, System.getenv().getOrDefault(KAFKA_MAX_REQUEST_SIZE, "4000000"));

    return config;
  }
}
