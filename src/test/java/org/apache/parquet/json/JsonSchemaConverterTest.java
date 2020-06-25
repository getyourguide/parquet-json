package org.apache.parquet.json;

import static org.junit.Assert.assertEquals;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.io.File;
import java.util.Objects;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

public class JsonSchemaConverterTest {

    @Test
    public void testConvertSimpleStringtype() throws Exception {

        String TypeName = "Sample0";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter();
        MessageType targetSchema = jsonSchemaConverter.convert(schema);

        System.out.println(targetSchema.toString());

        String expectedSchema =
                "message Sample0 {\n" +
                        "  optional binary key1 (STRING);\n" +
                        "  required binary key2 (STRING);\n" +
                        "}";

        assertEquals(MessageTypeParser.parseMessageType(expectedSchema).toString(), targetSchema.toString());
    }

    @Test
    public void testConvertSimpleInttype() throws Exception {
        String TypeName = "Test1";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter();
        MessageType targetSchema = jsonSchemaConverter.convert(schema);

        System.out.println(targetSchema.toString());

        String expectedSchema =
                "message Test1 {\n" +
                        "  optional INT32 key1;\n" +
                        "  optional INT64 key2;\n" +
                        "  optional INT32 key3 (INTEGER(16,false));\n" +
                        "}";

        assertEquals(MessageTypeParser.parseMessageType(expectedSchema).toString(), targetSchema.toString());
    }


}
