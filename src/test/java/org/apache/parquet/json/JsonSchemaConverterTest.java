package org.apache.parquet.json;

import static org.junit.Assert.assertEquals;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import java.io.File;
import java.util.Objects;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

public class JsonSchemaConverterTest {

    @Test
    public void testConvertPrimitiveTypes() throws Exception {
        String TypeName = "TestPrimitives";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter();
        MessageType targetSchema = jsonSchemaConverter.convert(schema);

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
                        "}";

        assertEquals(MessageTypeParser.parseMessageType(expectedSchema).toString(), targetSchema.toString());
    }

    @Test
    public void testConvertArraysofPrimitiveTypes() throws Exception {
        String TypeName = "TestArraysPrimitives";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter();
        MessageType targetSchema = jsonSchemaConverter.convert(schema);

        String expectedSchema =
                "message TestArraysPrimitives {\n"+
                        "  optional group array_string (LIST) {\n"+
                        "  repeated group list {\n"+
                        "    required binary element (STRING);\n"+
                        "    }\n"+
                        "  }\n"+
                        "  required group array_int (LIST) {\n"+
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

        assertEquals(MessageTypeParser.parseMessageType(expectedSchema).toString(), targetSchema.toString());
    }

    @Test
    public void TestNestedStructureTypes() throws Exception {
        String TypeName = "TestNestedStructure";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        ParseOptions options = new ParseOptions();
        options.setResolve(true);
        options.setFlatten(true);

        System.out.println(options);

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter();
        MessageType targetSchema = jsonSchemaConverter.convert(schema);

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

        assertEquals(MessageTypeParser.parseMessageType(expectedSchema).toString(), targetSchema.toString());

    }


}