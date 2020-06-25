package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.io.File;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.io.RecordConsumerLoggingWrapper;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonWriteSupportTest {
  private static final Logger LOG = LoggerFactory.getLogger(JsonWriteSupportTest.class);

  @Test
  public void testSimplestMessageStringOnly() throws Exception {
    String TypeName = "Test0";
    String resourceName = "openapi.yaml";

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
    String absolutePath = file.getAbsolutePath();

    OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
    ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

    RecordConsumerLoggingWrapper readConsumerMock = Mockito.mock(RecordConsumerLoggingWrapper.class);

    JsonWriteSupport support = new JsonWriteSupport(schema);
    support.init(new Configuration());
    support.prepareForWrite(readConsumerMock);

    String json = "{\"key1\":\"string1\", \"key2\":\"string2\"}";

    ObjectMapper mapper = new ObjectMapper();
    JsonNode payload = mapper.readTree(json);

    support.write(payload);

    System.out.println(readConsumerMock.toString());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();

    inOrder.verify(readConsumerMock).startField("key1", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("string1"));
    inOrder.verify(readConsumerMock).endField("key1", 0);

    inOrder.verify(readConsumerMock).startField("key2", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("string2"));
    inOrder.verify(readConsumerMock).endField("key2", 1);

    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);

  }

}
