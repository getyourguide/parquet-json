package org.apache.parquet.json;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import io.swagger.v3.oas.models.media.BinarySchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import java.util.Map;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.Builder;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaConverter.class);

  public JsonSchemaConverter() {}

  public MessageType convert(ObjectSchema jsonClass) {
    LOG.debug("Converting OpenAPI described JsonNode class \"" + jsonClass.getClass() + "\" to parquet schema.");
    MessageType messageType = convertFields(Types.buildMessage(), jsonClass.getProperties())
      .named(jsonClass.getTitle());
    return messageType;
  }

  /* Iterates over list of fields. **/
  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, Map<String, Schema> fieldDescriptors) {

    for ( Map.Entry<String, Schema> field: fieldDescriptors.entrySet() ) {
      groupBuilder =
        addField(field.getValue(), groupBuilder)
          .named(field.getKey());
    }

    return groupBuilder;
  }

  private Type.Repetition getRepetition(Schema descriptor) {

    if (descriptor.getNullable() != null && descriptor.getNullable()) {
      return Type.Repetition.OPTIONAL;
    } else if (descriptor.getNullable() != null && !descriptor.getNullable()) {
      return Repetition.REQUIRED;
    } else {
      //todo: is the default non nullable?
      return Type.Repetition.REQUIRED;
    }

  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(Schema descriptor, final GroupBuilder<T> builder) {

    ParquetType parquetType = getParquetType(descriptor);
    return builder.primitive(parquetType.primitiveType, getRepetition(descriptor)).as(parquetType.logicalTypeAnnotation);

  }

  private ParquetType getParquetType(Schema fieldSchema) {

    if (fieldSchema instanceof StringSchema) {
      return ParquetType.of(BINARY, stringType());
    }
    else if (fieldSchema instanceof IntegerSchema) {
      ParquetType IntType = ParquetType.of(INT32);
      switch (fieldSchema.getFormat()) {
        case "int16":
          IntType = ParquetType.of(INT32, LogicalTypeAnnotation.intType(16, false));
          break;
        case "int32":
          IntType = ParquetType.of(INT32);
          break;
        case "int64":
          IntType = ParquetType.of(INT64);
          break;
      }
      return IntType;
    }
    else if (fieldSchema instanceof BinarySchema) {
      return ParquetType.of(BINARY);
    }
    else {
      throw new UnsupportedOperationException("Cannot convert OpenAPI component: unknown type " +
        fieldSchema.getFormat().toLowerCase());
    }

  }

  private static class ParquetType {
    PrimitiveTypeName primitiveType;
    LogicalTypeAnnotation logicalTypeAnnotation;

    private ParquetType(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalTypeAnnotation) {
      this.primitiveType = primitiveType;
      this.logicalTypeAnnotation = logicalTypeAnnotation;
    }

    public static ParquetType of(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalTypeAnnotation) {
      return new ParquetType(primitiveType, logicalTypeAnnotation);
    }

    public static ParquetType of(PrimitiveTypeName primitiveType) {
      return of(primitiveType, null);
    }
  }

}
