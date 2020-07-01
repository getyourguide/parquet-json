package org.apache.parquet.json;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

public class JsonParquetWriterTest extends JsonParquetTest{

    @Test
    public void testWriteSimpleFile() throws Exception {

        Path path = new Path("./data.parquet");
        String TypeName = "TestPrimitives";
        JsonNode payload = getExample(TypeName);
        Schema schema = getSchema(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

        writer.write(payload);
        writer.close();

        File inFile = new File("data.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteComplexFile() throws Exception {

        Path path = new Path("./arrays.parquet");
        String TypeName = "TestArraysPrimitives";
        JsonNode payload = getExample(TypeName);
        Schema schema = getSchema(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

        writer.write(payload);
        writer.close();

        File inFile = new File("arrays.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteNestedFile() throws Exception {

        Path path = new Path("./nested.parquet");
        String TypeName = "TestNestedStructure";
        JsonNode payload = getExample(TypeName);
        Schema schema = getSchema(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

        writer.write(payload);
        writer.close();

        File inFile = new File("nested.parquet");

        assertTrue(inFile.exists());

    }

    @Test
    public void testWriteDeeperNested() throws Exception {

        Path path = new Path("./deep_nested.parquet");
        String TypeName = "TestDeeperNestedStructure";
        JsonNode payload = getExample(TypeName);
        Schema schema = getSchema(TypeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

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
