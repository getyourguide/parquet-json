package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import java.util.Objects;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import org.apache.hadoop.fs.Path;
import static org.junit.Assert.assertTrue;

public class JsonParquetWriterTest {

    @Test
    public void testWriteSimpleFile() throws Exception {

        Path path = new Path("./data.parquet");
        String TypeName = "Test0";
        String resourceName = "openapi.yaml";

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
        String absolutePath = file.getAbsolutePath();

        OpenAPI openAPI = new OpenAPIV3Parser().read(absolutePath);
        ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, schema);

        String json = "{\"key1\":\"string1\", \"key2\":\"string2\"}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = mapper.readTree(json);

        writer.write(payload);
        writer.close();

        File inFile = new File("data.parquet");

        assertTrue(inFile.exists());


    }


}
