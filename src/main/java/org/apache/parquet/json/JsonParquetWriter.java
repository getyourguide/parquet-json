package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.models.media.ObjectSchema;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

public class JsonParquetWriter<T> extends ParquetWriter<T> {

    public JsonParquetWriter(Path file, ObjectSchema jsonSchema) throws IOException {
        super(file, (WriteSupport<T>) new JsonWriteSupport<JsonNode>(jsonSchema));
    }

    JsonParquetWriter(Path file, WriteSupport<T> writeSupport,
                      CompressionCodecName compressionCodecName,
                      int blockSize, int pageSize, boolean enableDictionary,
                      boolean enableValidation, WriterVersion writerVersion,
                      Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    private static WriteSupport<JsonNode> writeSupport(Configuration conf,
                                                       ObjectSchema schema) {
        return new JsonWriteSupport(schema);
    }

    private static WriteSupport<JsonNode> writeSupport(Configuration conf,
                                                       ObjectSchema schema,
                                                       Boolean writeDefaultValue,
                                                       Boolean writeNullAsDefault) {
        return new JsonWriteSupport(schema, writeDefaultValue, writeNullAsDefault);
    }

    public static Builder<JsonNode> Builder(Path path) {
        return new Builder(path);
    }

    public static class Builder<JsonNode> extends ParquetWriter.Builder<JsonNode, Builder<JsonNode>> {

        private ObjectSchema schema = null;
        private Boolean writeDefaultValue;
        private Boolean writeNullAsDefault;

        protected Builder(Path path) {
            super(path);
        }

        protected Builder(OutputFile path) {
            super(path);
        }

        public Builder<JsonNode> withSchema(ObjectSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder<JsonNode> withNullAsDefault() {
            this.writeNullAsDefault = true;
            return this;
        }

        public Builder<JsonNode> withWriteDefault() {
            this.writeDefaultValue = true;
            return this;
        }

        @Override
        protected Builder<JsonNode> self() {
            return this;
        }

        @Override
        protected WriteSupport<JsonNode> getWriteSupport(Configuration conf) {
            return (WriteSupport<JsonNode>) JsonParquetWriter.writeSupport(conf, schema, writeDefaultValue, writeNullAsDefault);
        }
    }

}
