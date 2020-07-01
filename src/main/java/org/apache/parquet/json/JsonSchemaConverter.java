package org.apache.parquet.json;

import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.BinarySchema;
import io.swagger.v3.oas.models.media.BooleanSchema;
import io.swagger.v3.oas.models.media.DateSchema;
import io.swagger.v3.oas.models.media.DateTimeSchema;
import io.swagger.v3.oas.models.media.EmailSchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.PasswordSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.media.UUIDSchema;
import java.util.Map;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
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

    public JsonSchemaConverter() {
    }

    private <T> GroupBuilder<T> unsupportedSchema(Schema schema) {
        String exceptionMsg = "Unsupported type "+schema.getType();
        throw new InvalidSchemaException(exceptionMsg);
    }

    public MessageType convert(ObjectSchema jsonClass) {
        LOG.debug("Converting OpenAPI described JsonNode class \"" + jsonClass.getClass() + "\" to parquet schema.");
        MessageType messageType = convertFields(Types.buildMessage(), jsonClass.getProperties())
                .named(jsonClass.getTitle());
        return messageType;
    }

    /* Iterates over list of fields. **/
    private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, Map<String, Schema> fieldDescriptors) {

        int index = 0;
        for (Map.Entry<String, Schema> field : fieldDescriptors.entrySet()) {
            groupBuilder =
                    addField(field.getValue(), groupBuilder)
                            //.id(index)
                            .named(field.getKey());
            index++;
        }

        return groupBuilder;
    }

    private Type.Repetition getRepetition(Schema descriptor) {

        if (descriptor instanceof ArraySchema) {
            return Repetition.REPEATED;
        }

        if (descriptor.getNullable() != null && descriptor.getNullable()) {
            return Type.Repetition.OPTIONAL;
        } else if (descriptor.getNullable() != null && !descriptor.getNullable()) {
            return Repetition.REQUIRED;
        } else {
            // https://swagger.io/specification/
            // The default value is nullable=false
            return Repetition.REQUIRED;
        }

    }

    private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(Schema descriptor, final GroupBuilder<T> builder) {

        if (descriptor instanceof MapSchema) {
           return addMapField(descriptor, builder);
        }

        if (descriptor instanceof ObjectSchema) {
            return addObjectField((ObjectSchema) descriptor, builder);
        }

        if (descriptor instanceof ArraySchema) {
            Type.Repetition nullableField = Repetition.REQUIRED;
            Type.Repetition nullableItems;

            if (descriptor.getNullable() != null) {
                if (descriptor.getNullable()) {
                    nullableField = Repetition.OPTIONAL;
                }
            }

            nullableItems = getRepetition(((ArraySchema) descriptor).getItems());

            if (isPrimitiveType(((ArraySchema) descriptor).getItems())) {
                ParquetType parquetType = getParquetType(descriptor);

                return addRepeatedPrimitive(parquetType.primitiveType,
                        parquetType.logicalTypeAnnotation,
                        nullableField,
                        nullableItems,
                        builder);
            } else {
                return addRepeatedObject(((ArraySchema) descriptor).getItems(), builder);
            }
        }

        ParquetType parquetType = getParquetType(descriptor);
        return builder.primitive(parquetType.primitiveType, getRepetition(descriptor)).as(parquetType.logicalTypeAnnotation);

    }
    private <T> GroupBuilder<GroupBuilder<T>> addMapField(Schema descriptor, final GroupBuilder<T> builder) {
        if (descriptor.getAdditionalProperties() instanceof Boolean) {
            // Free-Form Objects are not supported
            return unsupportedSchema((Schema) descriptor.getAdditionalProperties());
        } else if (descriptor.getAdditionalProperties() instanceof Schema) {
            Schema mapValueSchema = (Schema) descriptor.getAdditionalProperties();

            GroupBuilder<GroupBuilder<GroupBuilder<T>>> group = builder
                    .group(getRepetition(descriptor)).as(mapType())
                    .group(Type.Repetition.REPEATED) // key_value wrapper
                    .primitive(BINARY, Type.Repetition.REQUIRED).as(stringType()).named("key"); // keys are always string in OPAI 3.0.x

            return addField(mapValueSchema, group).named("value")
                    .named("key_value");

        } else {
            return unsupportedSchema((Schema) descriptor.getAdditionalProperties());
        }
    }

    private <T> GroupBuilder<GroupBuilder<T>> addRepeatedObject(Schema schema, final GroupBuilder<T> builder) {
        GroupBuilder<GroupBuilder<GroupBuilder<T>>> group = builder
                .group(Repetition.OPTIONAL).as(listType()) //fix me
                .group(Repetition.REPEATED);

         return addField(schema, group).named("element").named("list");
    }

    private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addRepeatedPrimitive(PrimitiveTypeName primitiveType,
                                                                                                     LogicalTypeAnnotation logicalTypeAnnotation,
                                                                                                     Type.Repetition nullableField,
                                                                                                     Type.Repetition nullableItems,
                                                                                                     final GroupBuilder<T> builder) {
        return builder
                .group(nullableField).as(listType())
                .group(Repetition.REPEATED)
                .primitive(primitiveType, nullableItems).as(logicalTypeAnnotation)
                .named("element")
                .named("list");
    }

    private <T> GroupBuilder<GroupBuilder<T>> addObjectField(ObjectSchema field, final GroupBuilder<T> builder) {
        GroupBuilder<GroupBuilder<T>> group = builder.group(getRepetition(field));
        convertFields(group, field.getProperties());
        return group;
    }

    private Boolean isPrimitiveType(Schema fieldSchema) {
        if (fieldSchema instanceof ObjectSchema
                || fieldSchema instanceof ArraySchema
                || fieldSchema instanceof MapSchema) {
            return false;
        }
        return true;
    }

    private ParquetType getParquetType(Schema fieldSchema) {

        if (fieldSchema instanceof StringSchema || fieldSchema instanceof PasswordSchema || fieldSchema instanceof EmailSchema) {
            //todo: base64 encoded data as string could be mapped to the binary type
            return ParquetType.of(BINARY, stringType());
        } else if (fieldSchema instanceof UUIDSchema) {
            //todo: fix once PARQUET-1827 is released
            return ParquetType.of(BINARY, stringType());
        } else if (fieldSchema instanceof DateSchema) {
            return ParquetType.of(INT32, dateType()); //todo: writer
        } else if (fieldSchema instanceof DateTimeSchema) {
            return ParquetType.of(INT64, timestampType(true, TimeUnit.MILLIS)); //todo: writer
        } else if (fieldSchema instanceof IntegerSchema) {

            if (fieldSchema.getFormat() == null) {
                // If no format specified, we use int32
                return ParquetType.of(INT32);
            }

            switch (fieldSchema.getFormat()) {
                case "int16":
                    return ParquetType.of(INT32, LogicalTypeAnnotation.intType(16, false));
                case "int32":
                    return ParquetType.of(INT32);
                case "int64":
                    return ParquetType.of(INT64);
                default:
                    throw new UnsupportedOperationException("Cannot convert Integer: unknown format " +
                            fieldSchema.getFormat().toLowerCase());
            }
        } else if (fieldSchema instanceof BinarySchema) {
            return ParquetType.of(BINARY);
        } else if (fieldSchema instanceof BooleanSchema) {
            return ParquetType.of(BOOLEAN);
        } else if (fieldSchema instanceof NumberSchema) {

            if (fieldSchema.getFormat() == null) {
                // If no format specified, we use float
                return ParquetType.of(FLOAT);
            }

            switch (fieldSchema.getFormat()) {
                case "double":
                    return ParquetType.of(DOUBLE);
                case "float":
                    return ParquetType.of(FLOAT);
                default:
                    throw new UnsupportedOperationException("Cannot convert Number: unknown format " +
                            fieldSchema.getFormat().toLowerCase());
            }

        } else if (fieldSchema instanceof ArraySchema) {
            // ArraySchema will be marked as Repeated by the ParquetType function
            // what we are interested in here in to get the type of the elements
            return getParquetType(((ArraySchema) fieldSchema).getItems());
        } else {
            throw new UnsupportedOperationException("Cannot convert OpenAPI schema: unknown type " +
                    fieldSchema.getClass().getName());
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
