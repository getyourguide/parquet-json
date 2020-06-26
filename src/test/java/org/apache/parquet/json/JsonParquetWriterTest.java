package org.apache.parquet.json;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.io.File;
import java.util.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

public class JsonParquetWriterTest {

    @Test
    public void testWriteFile() throws Exception {

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

}
