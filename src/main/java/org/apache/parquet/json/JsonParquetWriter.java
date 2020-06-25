package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.models.media.ObjectSchema;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

public class JsonParquetWriter<T> extends ParquetWriter<T> {

    public JsonParquetWriter(Path file, ObjectSchema jsonSchema) throws IOException {
        super(file, (WriteSupport<T>) new JsonWriteSupport<JsonNode>(jsonSchema));
    }

}
