package org.folio.inventory.dataimport.handlers.actions;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle.TITLE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplaceInstanceEventHandlerTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String SOURCE_RECORDS_PATH = "/source-storage/records";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "dummy";
  private static final Integer INSTANCE_VERSION = 1;
  private static final String INSTANCE_VERSION_AS_STRING = "1";
  private static final String MARC_INSTANCE_SOURCE = "MARC";

  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceRecordCollection;
  @Mock
  private OkapiHttpClient mockedClient;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace preliminary Item")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  private ReplaceInstanceEventHandler replaceInstanceEventHandler;
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();

    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.toString())))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}" + "/formatted"), true))
      .willReturn(WireMock.ok(Json.encode(new Record()
        .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))))));

    precedingSucceedingTitlesHelper = Mockito.spy(new PrecedingSucceedingTitlesHelper(ctxt -> mockedClient));

    Vertx vertx = Vertx.vertx();
    replaceInstanceEventHandler = new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper, new MappingMetadataCache(vertx,
      vertx.createHttpClient(), 3600), vertx.createHttpClient());


    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).update(any(), any(Consumer.class), any(Consumer.class));

    doAnswer(invocationOnMock -> completedStage(createResponse(201, null)))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));
    doAnswer(invocationOnMock -> completedStage(createResponse(200, new JsonObject().encode())))
      .when(mockedClient).get(anyString());
    doAnswer(invocationOnMock -> completedStage(createResponse(204, null)))
      .when(mockedClient).delete(anyString());
  }

  @Test
  public void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern("/source-storage/records" + "/.*"), true)));
  }

  @Test
  public void shouldReplaceExistingPrecedingTitleOnInstanceUpdate() throws InterruptedException, ExecutionException {
    JsonObject existingPrecedingTitle = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put(TITLE_KEY, "Butterflies in the snow");

    JsonObject precedingSucceedingTitles = new JsonObject().put(PRECEDING_SUCCEEDING_TITLES_KEY, new JsonArray().add(existingPrecedingTitle));
    when(mockedClient.get(anyString()))
      .thenReturn(CompletableFuture.completedFuture(createResponse(HttpStatus.SC_OK, precedingSucceedingTitles.encode())));

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get();

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertThat(updatedInstance.getJsonArray("precedingTitles").size(), is(1));
    assertNotEquals(existingPrecedingTitle.getString(TITLE_KEY), updatedInstance.getJsonArray("precedingTitles").getJsonObject(0).getString(TITLE_KEY));
    assertThat(updatedInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));

    ArgumentCaptor<Set<String>> titleIdCaptor = ArgumentCaptor.forClass(Set.class);
    verify(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(titleIdCaptor.capture(), any(Context.class));
    assertTrue(titleIdCaptor.getValue().contains(existingPrecedingTitle.getString("id")));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsNull() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(null)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());;

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMArcBibliographicIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put("InvalidField", Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMarcBibliographicIsEmptyInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfRequiredFieldIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), MissingValue.getInstance());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(new JsonObject()))).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))));
    context.put(INSTANCE.value(), new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_MATCHED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());;

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfOLErrorExists() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    Instance returnedInstance = new Instance(UUID.randomUUID().toString(), String.valueOf(INSTANCE_VERSION), UUID.randomUUID().toString(), "source", "title", instanceTypeId);
    returnedInstance.setTags(List.of("firstTag"));
    when(instanceRecordCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(returnedInstance));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(20, TimeUnit.SECONDS);
  }

  @Test
  public void shouldNotRequestMarcRecordIfInstanceSourceIsNotMarc() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String newTitle = "test title";

    Reader mockedReader = Mockito.mock(Reader.class);
    when(mockedReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(newTitle));
    when(fakeReaderFactory.createReader()).thenReturn(mockedReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    JsonObject instanceJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", "FOLIO")
      .put("_version", INSTANCE_VERSION);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
        put(INSTANCE.value(), instanceJson.encode());
      }});

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertEquals(newTitle, createdInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    verify(0, getRequestedFor(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}"), true)));
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotInstance() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnTrue() {
    assertTrue(replaceInstanceEventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value(), replaceInstanceEventHandler.getPostProcessingInitializationEventType());
  }

  private Response createResponse(int statusCode, String body) {
    return new Response(statusCode, body, null, null);
  }
}
