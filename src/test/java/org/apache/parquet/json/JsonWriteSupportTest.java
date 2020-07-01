package org.apache.parquet.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.io.RecordConsumerLoggingWrapper;
import org.apache.parquet.io.api.Binary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonWriteSupportTest extends JsonParquetTest {
    private static final Logger LOG = LoggerFactory.getLogger(JsonWriteSupportTest.class);
    private RecordConsumerLoggingWrapper readConsumerMock;

    @Before
    public void init() {
     readConsumerMock = Mockito.mock(RecordConsumerLoggingWrapper.class);
    }

    private JsonWriteSupport getWriter(String schemaName) throws Exception {
        JsonWriteSupport support = new JsonWriteSupport(getSchema(schemaName));
        support.init(new Configuration());
        support.prepareForWrite(readConsumerMock);
        return support;
    }

    @Test
    public void testPrimitives() throws Exception {
        String TypeName = "TestPrimitives";

        JsonWriteSupport support = getWriter(TypeName);
        support.write(getExample(TypeName));

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

        JsonWriteSupport support = getWriter(TypeName);
        support.write(getExample(TypeName));

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

    //todo: TestArraysOfObjects

    @Test
    public void test1stLevelNestedStructure() throws Exception {
        String TypeName = "TestNestedStructure";

        JsonWriteSupport support = getWriter(TypeName);
        support.write(getExample(TypeName));

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

        JsonWriteSupport support = getWriter(TypeName);
        support.write(getExample(TypeName));

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

    @Test
    public void testMapSimpleStructure() throws Exception {
        String TypeName = "TestMapStructure";

        JsonWriteSupport support = getWriter(TypeName);
        support.write(getExample(TypeName));

        InOrder inOrder = Mockito.inOrder(readConsumerMock);
        inOrder.verify(readConsumerMock).startMessage();
        inOrder.verify(readConsumerMock).startField("map_key", 0);
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key_value", 0);

        // key1 group
        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("key", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("key1"));
        inOrder.verify(readConsumerMock).endField("key", 0);
        inOrder.verify(readConsumerMock).startField("value", 1);

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

        inOrder.verify(readConsumerMock).endField("value", 1);
        inOrder.verify(readConsumerMock).endGroup();

        // key2 group
        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("key", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("key2"));
        inOrder.verify(readConsumerMock).endField("key", 0);
        inOrder.verify(readConsumerMock).startField("value", 1);

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("list", 0);

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addInteger(4);
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addInteger(5);
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).startGroup();
        inOrder.verify(readConsumerMock).startField("element", 0);
        inOrder.verify(readConsumerMock).addInteger(6);
        inOrder.verify(readConsumerMock).endField("element", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("list", 0);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("value", 1);
        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("key_value", 0);

        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("map_key", 0);
        inOrder.verify(readConsumerMock).endMessage();
        Mockito.verifyNoMoreInteractions(readConsumerMock);
    }

    @Test
    public void testMapObjectStructure() throws Exception {
        String TypeName = "TestMapStructureofObject";

        JsonWriteSupport support = getWriter(TypeName);
        support.write(getExample(TypeName));

        InOrder inOrder = Mockito.inOrder(readConsumerMock);

        inOrder.verify(readConsumerMock).startMessage();

        inOrder.verify(readConsumerMock).startField("map_key", 0);
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key_value", 0);
        // key en
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("en"));
        inOrder.verify(readConsumerMock).endField("key", 0);

        inOrder.verify(readConsumerMock).startField("value", 1);
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("name", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("english"));
        inOrder.verify(readConsumerMock).endField("name", 0);

        inOrder.verify(readConsumerMock).startField("text", 1);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("hello"));
        inOrder.verify(readConsumerMock).endField("text", 1);

        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("value", 1);

        inOrder.verify(readConsumerMock).endGroup();

        // key de
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("key", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("de"));
        inOrder.verify(readConsumerMock).endField("key", 0);

        inOrder.verify(readConsumerMock).startField("value", 1);
        inOrder.verify(readConsumerMock).startGroup();

        inOrder.verify(readConsumerMock).startField("name", 0);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("german"));
        inOrder.verify(readConsumerMock).endField("name", 0);

        inOrder.verify(readConsumerMock).startField("text", 1);
        inOrder.verify(readConsumerMock).addBinary(Binary.fromString("hallo"));
        inOrder.verify(readConsumerMock).endField("text", 1);

        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("value", 1);

        inOrder.verify(readConsumerMock).endGroup();
        inOrder.verify(readConsumerMock).endField("key_value", 0);

        inOrder.verify(readConsumerMock).endGroup();

        inOrder.verify(readConsumerMock).endField("map_key", 0);
        inOrder.verify(readConsumerMock).endMessage();

        Mockito.verifyNoMoreInteractions(readConsumerMock);
    }

}
