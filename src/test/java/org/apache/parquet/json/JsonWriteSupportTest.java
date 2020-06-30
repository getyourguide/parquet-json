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
    public void testPrimitives() throws Exception {
        String TypeName = "TestPrimitives";
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

        String json = "{\"key_string\":\"hello\",\"key_int32\":32,\"key_int64\":64,\"key_float\":10.10,\"key_double\":10.1010,\"is_true\":true,\"date\":\"2020-06-20\",\"datetime\":\"2020-06-20T10:10:10.000Z\"}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        support.write(payload);

        System.out.println(readConsumerMock.toString());

        InOrder inOrder = Mockito.inOrder(readConsumerMock);

        inOrder.verify(readConsumerMock).startMessage();

        inOrder.verify(readConsumerMock).startField("key_string", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("hello"));
        inOrder.verify(readConsumerMock).endField("key_string", 0);

        inOrder.verify(readConsumerMock).startField("key_int32", 1);
        inOrder.verify(readConsumerMock).addInteger(32);
        inOrder.verify(readConsumerMock).endField("key_int32", 1);

        inOrder.verify(readConsumerMock).startField("key_int64", 2);
        inOrder.verify(readConsumerMock).addLong(64);
        inOrder.verify(readConsumerMock).endField("key_int64", 2);

        inOrder.verify(readConsumerMock).startField("key_float", 3);
        inOrder.verify(readConsumerMock).addFloat((float) 10.10);
        inOrder.verify(readConsumerMock).endField("key_float", 3);

        inOrder.verify(readConsumerMock).startField("key_double", 4);
        inOrder.verify(readConsumerMock).addDouble(10.101);
        inOrder.verify(readConsumerMock).endField("key_double", 4);

        inOrder.verify(readConsumerMock).startField("is_true", 5);
        inOrder.verify(readConsumerMock).addBoolean(true);
        inOrder.verify(readConsumerMock).endField("is_true", 5);

        inOrder.verify(readConsumerMock).startField("date", 6);
        inOrder.verify(readConsumerMock).addInteger(18433);
        inOrder.verify(readConsumerMock).endField("date", 6);

        inOrder.verify(readConsumerMock).startField("datetime", 7);
        inOrder.verify(readConsumerMock).addLong(1592647810000L);
        inOrder.verify(readConsumerMock).endField("datetime", 7);

        inOrder.verify(readConsumerMock).endMessage();
        Mockito.verifyNoMoreInteractions(readConsumerMock);

    }

    @Test
    public void testArraysOfPrimitives() throws Exception {
        String TypeName = "TestArraysPrimitives";
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

        String json = "{\"array_string\":[\"hello\",\"bonjour\",\"gruezi\",\"hallo\"]}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        support.write(payload);
        System.out.println(readConsumerMock.toString());

        InOrder inOrder = Mockito.inOrder(readConsumerMock);

        inOrder.verify(readConsumerMock).startMessage();

        inOrder.verify(readConsumerMock).startField("array_string", 0);
        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("list", 0);

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("hello"));
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("bonjour"));
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("gruezi"));
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("hallo"));
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("list", 0);
        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("array_string", 0);

        inOrder.verify(readConsumerMock).endMessage();
        Mockito.verifyNoMoreInteractions(readConsumerMock);
    }

    @Test
    public void test1stLevelNestedStructure() throws Exception {
        String TypeName = "TestNestedStructure";
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

        String json = "{\"simple_nested\":{\"key1\":\"2020-06-20\",\"key2\":[1,2,3]}}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        support.write(payload);
        System.out.println(readConsumerMock.toString());

        InOrder inOrder = Mockito.inOrder(readConsumerMock);

        inOrder.verify(readConsumerMock).startMessage();
        inOrder.verify(readConsumerMock).startField("simple_nested", 1);

        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key1", 0);
        inOrder.verify(readConsumerMock).addInteger(18433);
        inOrder.verify(readConsumerMock).endField("key1", 0);


        inOrder.verify(readConsumerMock).startField("key2", 1);

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("list", 0);

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addInteger(1);
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addInteger(2);
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addInteger(3);
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("list", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("key2", 1);

        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("simple_nested", 1);
        inOrder.verify(readConsumerMock).endMessage();

        Mockito.verifyNoMoreInteractions(readConsumerMock);
    }

    @Test
    public void test2stLevelNestedStructure() throws Exception {
        String TypeName = "TestDeeperNestedStructure";
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

        String json = "{\"1st_level_key1\":\"Hello\",\"1st_level_key_nested\":{\"key1\":{\"key1_key1\":\"Bonjour\",\"key1_key2\":\"Guten Tag!\"},\"key2\":\"Olla!\"}}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        support.write(payload);
        System.out.println(readConsumerMock.toString());

        InOrder inOrder = Mockito.inOrder(readConsumerMock);

        inOrder.verify(readConsumerMock).startMessage();
        inOrder.verify(readConsumerMock).startField("1st_level_key1", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("Hello"));
        inOrder.verify(readConsumerMock).endField("1st_level_key1", 0);

        inOrder.verify(readConsumerMock).startField("1st_level_key_nested", 1);
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key1", 0);
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key1_key1", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("Bonjour"));
        inOrder.verify(readConsumerMock).endField("key1_key1", 0);

        inOrder.verify(readConsumerMock).startField("key1_key2", 1);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("Guten Tag!"));
        inOrder.verify(readConsumerMock).endField("key1_key2", 1);

        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("key1", 0);

        inOrder.verify(readConsumerMock).startField("key2", 1);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("Olla!"));
        inOrder.verify(readConsumerMock).endField("key2", 1);

        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("1st_level_key_nested", 1);

        inOrder.verify(readConsumerMock).endMessage();

        Mockito.verifyNoMoreInteractions(readConsumerMock);

    }

}
