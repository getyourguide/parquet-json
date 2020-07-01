package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.io.File;
import java.util.Objects;
import org.junit.BeforeClass;

public class JsonParquetTest {
    private static OpenAPI openAPI;

    @BeforeClass
    public static void setup() {
        String resourceName = "openapi.yaml";
        ClassLoader classLoader = JsonSchemaConverterTest.class.getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();
        openAPI = new OpenAPIV3Parser().read(absolutePath);
    }

    public ObjectSchema getSchema(String schemaName) throws Exception {

        if (openAPI.getComponents().getSchemas().get(schemaName) == null) {
            throw new Exception(schemaName+" not found");
        }

        return (ObjectSchema) openAPI.getComponents().getSchemas().get(schemaName);
    }

    public JsonNode getExample(String schemaName) throws Exception {
        ObjectSchema schema = getSchema(schemaName);
        if (schema.getExample() instanceof ObjectNode) {
            return (JsonNode) schema.getExample();
        } else if (schema.getExample() instanceof ArrayNode) {
            ArrayNode examples = (ArrayNode) schema.getExample();
            return examples.get(0);
        } else {
            throw new Exception("Unexpected example data type "+schema.getType());
        }
    }
}
