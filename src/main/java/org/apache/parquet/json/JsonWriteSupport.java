package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.util.Json;
import io.swagger.v3.oas.models.media.ArraySchema;
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
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link WriteSupport} for writing JSON from Jackson JsonNode.
 */
public class JsonWriteSupport<T extends JsonNode> extends WriteSupport<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonWriteSupport.class);

    private RecordConsumer recordConsumer;
    private ObjectSchema objectSchema;
    private MessageWriter messageWriter;

    public JsonWriteSupport() {

    }

    public JsonWriteSupport(ObjectSchema objSchema) {
        this.objectSchema = objSchema;
    }

    @Override
    public String getName() {
        return "json";
    }

    @Override
    public WriteContext init(Configuration configuration) {
        MessageType rootSchema = new JsonSchemaConverter().convert(objectSchema);
        this.messageWriter = new MessageWriter(objectSchema, rootSchema);
        Map<String, String> extraMetaData = new HashMap<String, String>();
        return new WriteContext(rootSchema, extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(T record) {
        recordConsumer.startMessage();

        try {
            messageWriter.writeTopLevelMessage(record);
        } catch (RuntimeException e) {
            LOG.error("Cannot write message " + e.getMessage() + " : " + record);
            throw e;
        }

        recordConsumer.endMessage();
    }

    private FieldWriter unknownType(Schema fieldDescriptor) {
        String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor
                + "\" and type \"" + fieldDescriptor.getType() + "\".";
        throw new InvalidRecordException(exceptionMsg);
    }

    class FieldWriter {
        String fieldName;
        int index = -1;

        void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        void setIndex(int index) {
            this.index = index;
        }

        void writeRawValue(Object value) {
        }

        void writeField(Object value) {

            if (value instanceof NullNode) {
                LOG.debug("Null value");
                return;
            }

            recordConsumer.startField(fieldName, index);
            writeRawValue(value);
            recordConsumer.endField(fieldName, index);
        }

    }

    class MessageWriter extends FieldWriter {
        final FieldWriter[] fieldWriters;
        ObjectSchema messageObjectSchema;

        @SuppressWarnings("unchecked")
        MessageWriter(ObjectSchema objSchema, GroupType schema) {

            this.messageObjectSchema = objSchema;
            int fieldsSize = messageObjectSchema.getProperties().entrySet().size();
            fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fieldsSize);

            int fieldIndex = 0;
            for (Map.Entry<String, Schema> field : messageObjectSchema.getProperties().entrySet()) {

                String name = field.getKey();
                Type type = schema.getType(name);
                FieldWriter writer = createWriter(field.getValue(), type);

                LOG.debug("Field {} has index {}", name, fieldIndex);
                writer.setFieldName(name);
                writer.setIndex(fieldIndex);

                fieldWriters[fieldIndex] = writer;

                fieldIndex++;
            }

        }

        private MessageWriter CreateObjectWriter(Schema field, Type type) {
            return new MessageWriter((ObjectSchema) field, type.asGroupType());
        }

        private ArrayWriter CreateArrayWriter(Schema field, Type type) {
            FieldWriter itemWriter;

            Schema itemSchema = ((ArraySchema) field).getItems();

            // Array of objects
            if (itemSchema instanceof ObjectSchema) {
                Type innerType = type
                        .asGroupType()
                        .getType("list")
                        .asGroupType()
                        .getType("element");
                itemWriter = createWriter(itemSchema, innerType);
            } else if (itemSchema instanceof MapSchema) {
                LOG.error("Array of maps is not supported");
                return (ArrayWriter) unknownType(itemSchema);
            }
            else {
                // Array of primitive type
                itemWriter = createWriter(itemSchema, type);
            }

            return new ArrayWriter(itemWriter);
        }

        private MapWriter CreateMapWrite(Schema field, Type type) {

            StringSchema keySchema = new StringSchema();
            FieldWriter keyWriter = createWriter(keySchema, null); // with OPAI map always have string keys
            FieldWriter valueWriter;
            keyWriter.setFieldName("key");
            keyWriter.setIndex(0);

            Schema valueSchema = (Schema) field.getAdditionalProperties();

            // we will assume that we won't get a "Free-Form Objects"
            if (valueSchema instanceof ObjectSchema) {
                Type innerType = type
                        .asGroupType()
                        .getType("key_value")
                        .asGroupType()
                        .getType("value");
                valueWriter = createWriter(valueSchema, innerType);
            } else {
                valueWriter = createWriter(valueSchema, type);
            }

            valueWriter.setIndex(1);
            valueWriter.setFieldName("value");
            return new MapWriter(keyWriter, valueWriter);
        }

        private FieldWriter createWriter(Schema field, Type type) {

            if (field instanceof StringSchema || field instanceof PasswordSchema || field instanceof EmailSchema) {
                return new StringWriter();
            } else if (field instanceof UUIDSchema) {
                //todo: fix once PARQUET-1827 is released
                return new StringWriter();
            } else if (field instanceof DateSchema) {
                return new DateWriter();
            } else if (field instanceof DateTimeSchema) {
                return new DateTimeWriter();
            } else if (field instanceof IntegerSchema) {

                if (field.getFormat() == null) {
                    return new IntWriter();
                }

                if (field.getFormat().toLowerCase().equals("int32")) {
                    return new IntWriter();
                } else if (field.getFormat().toLowerCase().equals("int64")) {
                    return new LongWriter();
                } else if (field.getFormat().toLowerCase().equals("int16")) {
                    return new IntWriter();
                } else {
                    return unknownType(field);
                }
            } else if (field instanceof BooleanSchema) {
                return new BooleanWriter();
            } else if (field instanceof NumberSchema) {

                if (field.getFormat() == null) {
                    return new FloatWriter();
                }

                if (field.getFormat().toLowerCase().equals("float")) {
                    return new FloatWriter();
                } else if (field.getFormat().toLowerCase().equals("double")) {
                    return new DoubleWriter();
                } else {
                    return unknownType(field);
                }

            } else if (field instanceof ArraySchema) {
                return CreateArrayWriter(field, type);
            } else if (field instanceof ObjectSchema) {
                return CreateObjectWriter(field, type);
            } else if (field instanceof MapSchema) {
                return CreateMapWrite(field, type);
            }
            else {
                //todo: all other cases
                return unknownType(field); //should not be executed, always throws exception.
            }

        }

        /**
         * Writes top level message. It cannot call startGroup()
         */
        void writeTopLevelMessage(Object value) {
            writeAllFields((JsonNode) value);
        }

        // Use to write an ObjectNode (nested structure)
        @Override
        final void writeRawValue(Object value) {
            recordConsumer.startGroup();
            writeAllFields((ObjectNode) value);
            recordConsumer.endGroup();
        }

        private void writeAllFields(JsonNode pb) {

            int fieldIndex = 0;
            // objectSchema doesn't map to the right schema, it maps to the root schema
            for (Map.Entry<String, Schema> field : messageObjectSchema.getProperties().entrySet()) {

                String lkpFieldName = field.getKey();

                LOG.debug("Looking for {}", lkpFieldName);

                if (pb.has(lkpFieldName)) {
                    JsonNode node = pb.get(lkpFieldName);
                    LOG.debug(lkpFieldName + " is " + node.toPrettyString());

                    if (!node.isMissingNode()) {
                        LOG.debug("Writing field {}", lkpFieldName);
                        fieldWriters[fieldIndex].writeField(node);
                    }
                }

                // todo: decide if we write default value instead, in which case we need to call the
                fieldIndex++; //todo: compute some index based on schema

            }
        }

    }

    class StringWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {

            if (value instanceof JsonNode) {
                JsonNode node = (JsonNode) value;

                if (node.isTextual()) {
                    Binary binaryString = Binary.fromString(node.asText());
                    recordConsumer.addBinary(binaryString);
                } else {
                    LOG.error("Type {} not expected in {}", StringWriter.class.getName(), node.getNodeType());
                }
            } else {
                String strValue = (String) value;
                recordConsumer.addBinary(Binary.fromString(strValue));
            }

        }
    }

    class DateWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;

            if (node.isTextual()) {
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date
                LocalDate ts = LocalDate.parse(node.asText());
                long noOfDaysBetween = ChronoUnit.DAYS.between(LocalDate.of(1970, Month.JANUARY, 1), ts);
                recordConsumer.addInteger((int) noOfDaysBetween);
            } else {
                LOG.error("Type {} not expected in {}", DateWriter.class.getName(), node.getNodeType());
            }
        }
    }

    class DateTimeWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;

            if (node.isTextual()) {
                OffsetDateTime ts = OffsetDateTime.parse(node.asText());
                recordConsumer.addLong(ts.toInstant().toEpochMilli());
            } else {
                LOG.error("Type {} not expected in {}", DateTimeWriter.class.getName(), node.getNodeType());
            }
        }
    }

    class IntWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;

            if (node.isInt()) {
                recordConsumer.addInteger(node.asInt());
            } else {
                LOG.error("Type {} not expected in {}", IntWriter.class.getName(), node.getNodeType());
            }
        }
    }

    class LongWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;

            if (node.isNumber()) {
                recordConsumer.addLong(node.asLong());
            } else {
                LOG.error("Type {} not expected in {}", LongWriter.class.getName(), node.getNodeType());
            }
        }
    }

    class BooleanWriter extends FieldWriter {
        @Override
        void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;

            if (node.isBoolean()) {
                recordConsumer.addBoolean(node.asBoolean());
            } else {
                LOG.error("Type {} not expected in {}", BooleanWriter.class.getName(), node.getNodeType());
            }

        }
    }

    class FloatWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;
            recordConsumer.addFloat((float) node.asDouble());
        }
    }

    class DoubleWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;
            recordConsumer.addDouble(node.asDouble());
        }
    }

    class ArrayWriter extends FieldWriter {
        final FieldWriter fieldWriter;

        ArrayWriter(FieldWriter fieldWriter) {
            this.fieldWriter = fieldWriter;
        }

        @Override
        final void writeRawValue(Object value) {
            throw new UnsupportedOperationException("Array has no raw value");
        }

        @Override
        final void writeField(Object value) {

            ArrayNode node = (ArrayNode) value;

            if (node.size() == 0) {
                return;
            }

            recordConsumer.startField(fieldName, index);
            recordConsumer.startGroup();

            recordConsumer.startField("list", 0); // This is the wrapper group for the array field
            for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
                Object listEntry = it.next();
                recordConsumer.startGroup();
                recordConsumer.startField("element", 0); // This is the mandatory inner field

                fieldWriter.writeRawValue(listEntry);

                recordConsumer.endField("element", 0);
                recordConsumer.endGroup();
            }
            recordConsumer.endField("list", 0);

            recordConsumer.endGroup();
            recordConsumer.endField(fieldName, index);
        }
    }

    class MapWriter extends FieldWriter {
        private final FieldWriter keyWriter;
        private final FieldWriter valueWriter;

        public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
            super();
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
        }

        @Override
        final void writeRawValue(Object value) {

            JsonNode node = (JsonNode) value;

            if (node.isMissingNode() || node instanceof NullNode) {
                return;
            }

            recordConsumer.startGroup();

            recordConsumer.startField("key_value", 0); // This is the wrapper group for the map field

            for (Iterator<Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Entry<String, JsonNode> field = it.next();

                String mapKey = field.getKey();
                JsonNode mapValue = field.getValue();

                recordConsumer.startGroup();

                //todo: remove me
                LOG.debug("KEY {}", mapKey);

                keyWriter.writeField(mapKey);
                valueWriter.writeField(mapValue);

                recordConsumer.endGroup();

            }

            recordConsumer.endField("key_value", 0);

            recordConsumer.endGroup();
        }
    }

}
