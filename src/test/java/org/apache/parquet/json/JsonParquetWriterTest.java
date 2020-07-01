package org.apache.parquet.json;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

public class JsonParquetWriterTest extends JsonParquetTest{

    @Test
    public void testWriteSimpleFile() throws Exception {

        Path path = new Path("./data.parquet");
        String TypeName = "TestPrimitives";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, schema);

        String json = "{\"key_string\":\"hello\",\"key_int32\":32,\"key_int64\":64,\"key_float\":10.10,\"key_double\":10.1010,\"is_true\":true,\"date\":\"2020-06-20\",\"datetime\":\"2020-06-20T10:10:10.000Z\"}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        writer.write(payload);
        writer.close();

        File inFile = new File("data.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteComplexFile() throws Exception {

        Path path = new Path("./arrays.parquet");
        String TypeName = "TestArraysPrimitives";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, schema);

        String json = "{\"array_string\":[\"hello\",\"bonjour\",\"gruezi\",\"hallo\"]}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        writer.write(payload);
        writer.close();

        File inFile = new File("arrays.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteNestedFile() throws Exception {

        Path path = new Path("./nested.parquet");
        String TypeName = "TestNestedStructure";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, schema);

        String json = "{\"simple_nested\":{\"key1\":\"2020-06-20\",\"key2\":[1,2,3]}}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        writer.write(payload);
        writer.close();

        File inFile = new File("nested.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteDeeperNested() throws IOException {

        Path path = new Path("./deep_nested.parquet");
        String TypeName = "TestDeeperNestedStructure";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, schema);

        String json = "{\"1st_level_key1\":\"Hello\",\"1st_level_key_nested\":{\"key1\":{\"key1_key1\":\"Bonjour\",\"key1_key2\":\"Guten Tag!\"},\"key2\":\"Olla!\"}}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        writer.write(payload);
        writer.close();

        File inFile = new File("deep_nested.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteMap() throws Exception {
        String TypeName = "TestMapStructure";
        JsonNode payload = getExample(TypeName);
        Schema schema = getSchema(TypeName);
        Path path = new Path("./map.parquet");

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

        writer.write(payload);
        writer.close();

        File inFile = new File("map.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteMapOfObjects() throws Exception {
        String TypeName = "TestMapStructureofObject";
        JsonNode payload = getExample(TypeName);
        Schema schema = getSchema(TypeName);
        Path path = new Path("./map_obj.parquet");

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

        writer.write(payload);
        writer.close();

        File inFile = new File("map_obj.parquet");

        assertTrue(inFile.exists());

    }

}
