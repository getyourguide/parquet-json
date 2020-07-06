package org.getyourguide.parquet.json;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.parquet.hadoop.ParquetOutputFormat;

public class JsonParquetOutputFormat<T extends JsonNode> extends ParquetOutputFormat<T> {

}
