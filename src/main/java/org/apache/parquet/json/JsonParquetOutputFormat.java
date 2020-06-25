package org.apache.parquet.json;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;

public class JsonParquetOutputFormat<T extends JsonNode> extends ParquetOutputFormat<T> {

}
