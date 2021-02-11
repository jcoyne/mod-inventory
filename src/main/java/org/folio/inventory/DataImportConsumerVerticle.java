package org.folio.inventory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventTypes;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.processing.events.EventManager;
import org.folio.util.pubsub.PubSubClientUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_ENV;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_HOST;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_PORT;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.KAFKA_REPLICATION_FACTOR;
import static org.folio.inventory.dataimport.util.KafkaConfigConstants.OKAPI_URL;

public class DataImportConsumerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataImportConsumerVerticle.class);
  private static final List<DataImportEventTypes> EVENT_TYPES = List.of(DI_SRS_MARC_BIB_RECORD_CREATED,
    DI_SRS_MARC_BIB_RECORD_MODIFIED, DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING,
    DI_SRS_MARC_BIB_RECORD_MATCHED, DI_SRS_MARC_BIB_RECORD_NOT_MATCHED,
    DI_INVENTORY_INSTANCE_CREATED, DI_INVENTORY_INSTANCE_UPDATED,
    DI_INVENTORY_INSTANCE_MATCHED, DI_INVENTORY_INSTANCE_NOT_MATCHED,
    DI_INVENTORY_HOLDING_CREATED, DI_INVENTORY_HOLDING_UPDATED,
    DI_INVENTORY_HOLDING_MATCHED, DI_INVENTORY_HOLDING_NOT_MATCHED,
    DI_INVENTORY_ITEM_CREATED, DI_INVENTORY_ITEM_MATCHED,
    DI_INVENTORY_ITEM_NOT_MATCHED, DI_INVENTORY_ITEM_CREATED);

  private final int loadLimit = getLoadLimit();
  private final int maxDistributionNumber = getMaxDistributionNumber();
  private List<KafkaConsumerWrapper<String, String>> consumerWrappers = new ArrayList<>();

  @Override
  public void start(Promise<Void> startPromise) {
    JsonObject config = vertx.getOrCreateContext().config();
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(config.getString(KAFKA_ENV))
      .kafkaHost(config.getString(KAFKA_HOST))
      .kafkaPort(config.getString(KAFKA_PORT))
      .okapiUrl(config.getString(OKAPI_URL))
      .replicationFactor(Integer.parseInt(config.getString(KAFKA_REPLICATION_FACTOR)))
      .build();
    LOGGER.info(format("kafkaConfig: %s", kafkaConfig));
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, maxDistributionNumber);

    HttpClient client = vertx.createHttpClient();
    Storage storage = Storage.basedUpon(vertx, config, client);
    DataImportKafkaHandler dataImportKafkaHandler = new DataImportKafkaHandler(vertx, storage, client);

    List<Future> futures = EVENT_TYPES.stream()
      .map(eventType -> createKafkaConsumerWrapper(kafkaConfig, eventType, dataImportKafkaHandler))
      .collect(Collectors.toList());

    CompositeFuture.all(futures)
      .onFailure(startPromise::fail)
      .onSuccess(ar -> {
        futures.forEach(future -> consumerWrappers.add((KafkaConsumerWrapper<String, String>) future.result()));
        startPromise.complete();
      });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    List<Future> stopFutures = consumerWrappers.stream()
      .map(KafkaConsumerWrapper::stop)
      .collect(Collectors.toList());

    CompositeFuture.join(stopFutures).onComplete(ar -> stopPromise.complete());
  }

  private Future<KafkaConsumerWrapper<String, String>> createKafkaConsumerWrapper(KafkaConfig kafkaConfig, DataImportEventTypes eventType,
                                                                                  AsyncRecordHandler<String, String> recordHandler) {
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(),
      KafkaTopicNameHelper.getDefaultNameSpace(), eventType.value());

    KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    return consumerWrapper.start(recordHandler, PubSubClientUtils.constructModuleName())
      .map(consumerWrapper);
  }

  private int getLoadLimit() {
    return Integer.parseInt(System.getProperty("inventory.kafka.DataImportConsumer.loadLimit", "5"));
  }

  private int getMaxDistributionNumber() {
    return Integer.parseInt(System.getProperty("inventory.kafka.DataImportConsumerVerticle.maxDistributionNumber", "100"));
  }
}