package org.getyourguide.parquet.json;

import static org.junit.Assert.assertEquals;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.io.File;
import java.util.Objects;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonSchemaConverterTest {

    private static OpenAPI openAPI;

    @BeforeClass
    public static void setup() {
        String resourceName = "openapi.yaml";
        ClassLoader classLoader = JsonSchemaConverterTest.class.getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();
        openAPI = new OpenAPIV3Parser().read(absolutePath);
    }

    private ObjectSchema getSchema(String schemaName) throws Exception {

        if (openAPI.getComponents().getSchemas().get(schemaName) == null) {
            throw new Exception(schemaName+" not found");
        }

        return (ObjectSchema) openAPI.getComponents().getSchemas().get(schemaName);
    }

    private void testConversion(String TypeName, String expectedSchema) throws Exception {

        ObjectSchema schema = getSchema(TypeName);
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter();
        MessageType targetSchema = jsonSchemaConverter.convert(schema);

        System.out.println(targetSchema); //todo: remove

        assertEquals(MessageTypeParser.parseMessageType(expectedSchema).toString(), targetSchema.toString());
    }

    @Test
    public void testConvertPrimitiveTypes() throws Exception {
        String TypeName = "TestPrimitives";
        String expectedSchema =
                "message TestPrimitives {\n" +
                        "  required BINARY key_string (STRING);\n" +
                        "  optional INT32 key_int32;\n" +
                        "  optional INT64 key_int64;\n" +
                        "  optional FLOAT key_float;\n" +
                        "  optional DOUBLE key_double;\n" +
                        "  optional BOOLEAN is_true;\n" +
                        "  optional INT32 date (DATE);\n" + //will have annotation
                        "  optional INT64 datetime (TIMESTAMP(MILLIS,true));\n" +
                        "  optional BINARY key_bytes_from_string;\n"+
                        "}";

        testConversion(TypeName, expectedSchema);
    }

    @Test
    public void testConvertArrayOfPrimitiveTypes() throws Exception {
        String TypeName = "TestArraysPrimitives";
        String expectedSchema =
                "message TestArraysPrimitives {\n"+
                        "  optional group array_string (LIST) {\n"+
                        "  repeated group list {\n"+
                        "    required binary element (STRING);\n"+
                        "    }\n"+
                        "  }\n"+
                        "  optional group array_int (LIST) {\n"+
                        "  repeated group list {\n"+
                        "    optional INT64 element;\n"+
                        "    }\n"+
                        "  }\n"+
                        "  optional group array_bool (LIST) {\n"+
                        "  repeated group list {\n"+
                        "    optional BOOLEAN element;\n"+
                        "    }\n"+
                        "  }\n"+
                        "}";

        testConversion(TypeName, expectedSchema);
    }

    @Test
    public void testConvertArraysOfObjectsTypes() throws Exception {
        String TypeName = "TestArraysOfObjects";
        String expectedSchema =
                "message TestArraysOfObjects {\n"+
                        "  optional group array_key (LIST) {\n"+
                        "    repeated group list {\n"+
                        "      required group element {\n"+
                        "        required binary key_a (STRING);\n"+
                        "        required binary key_b (STRING);\n"+
                        "      }\n"+
                        "    }\n"+
                        "  }\n"+
                        "}\n";

        testConversion(TypeName, expectedSchema);
    }

    @Test
    public void TestNestedStructureTypes() throws Exception {
        String TypeName = "TestNestedStructure";
        String expectedSchema =
                "message TestNestedStructure {\n"+
                        "  optional int32 simple_key;\n"+
                        "  required group simple_nested {\n"+
                        "    required int32 key1 (DATE);\n"+
                        "    required group key2 (LIST) {\n"+
                        "      repeated group list {\n"+
                        "        required int32 element;\n"+
                        "      }\n"+
                        "    }\n"+
                        "  }\n"+
                        "}";

        testConversion(TypeName, expectedSchema);
    }

    @Test
    public void TestMapStructureType() throws Exception {
        String TypeName = "TestMapStructure";
        String expectedSchema =
                "message TestMapStructure {\n"+
                        "  required group map_key (MAP) {\n"+
                        "    repeated group key_value {\n"+
                        "      required binary key (STRING);\n"+
                        "      required group value (LIST) {\n"+
                        "        repeated group list {\n"+
                        "          required int32 element;\n"+
                        "        }\n"+
                        "      }\n"+
                        "    }\n"+
                        "  }\n"+
                        "}\n";

        testConversion(TypeName, expectedSchema);
    }

    @Test
    public void TestMapStructureOfObjectType() throws Exception {
        String TypeName = "TestMapStructureofObject";
        String expectedSchema =
                "message TestMapStructureofObject {\n"+
                        "  required group map_key (MAP) {\n"+
                        "    repeated group key_value {\n"+
                        "      required binary key (STRING);\n"+
                        "      required group value {\n"+
                        "        required binary name (STRING);\n"+
                        "        required binary text (STRING);\n"+
                        "      }\n"+
                        "    }\n"+
                        "  }\n"+
                        "}\n";

        testConversion(TypeName, expectedSchema);
    }

}