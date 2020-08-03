package org.getyourguide.parquet.json;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class JsonParquetWriterTest extends JsonParquetTest{

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private String getFullPath(String filename) {
        return folder.getRoot()+"/"+filename;
    }

    private void testFile(String typeName) throws Exception {
        String file = getFullPath(typeName+".parquet");

        Path path = new Path(file);

        JsonNode payload = getExample(typeName);
        Schema schema = getSchema(typeName);

        ParquetWriter<JsonNode> writer =
                new JsonParquetWriter(path, (ObjectSchema) schema);

        writer.write(payload);
        writer.close();

        File inFile = new File(file);
        assertTrue(inFile.exists());
    }

    @Test
    public void testWriteSimpleFile() throws Exception {
        testFile("TestPrimitives");
    }

    @Test
    public void testWriteComplexFile() throws Exception {
        testFile("TestArraysPrimitives");
    }

    @Test
    public void testWriteNestedFile() throws Exception {
        testFile("TestNestedStructure");
    }

    @Test
    public void testWriteDeeperNested() throws Exception {
        testFile("TestDeeperNestedStructure");
    }

    @Test
    public void testWriteMap() throws Exception {
        testFile("TestMapStructure");
    }

    @Test
    public void testWriteMapOfObjects() throws Exception {
        testFile("TestMapStructureofObject");
    }

    @Test
    public void testMissingInPayload() throws Exception {
        exceptionRule.expect(RequiredFieldException.class);
        testFile("NullInPayload");
    }


}
