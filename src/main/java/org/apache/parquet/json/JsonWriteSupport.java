package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.BooleanSchema;
import io.swagger.v3.oas.models.media.DateSchema;
import io.swagger.v3.oas.models.media.DateTimeSchema;
import io.swagger.v3.oas.models.media.EmailSchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.PasswordSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.media.UUIDSchema;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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

    public static final String JS_CLASS_WRITE = "parquet.json.writeClass";
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

    class MessageWriter extends FieldWriter {
        final FieldWriter[] fieldWriters;

        @SuppressWarnings("unchecked")
        MessageWriter(ObjectSchema objectSchema, GroupType schema) {
            int fieldsSize = objectSchema.getProperties().entrySet().size();
            fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fieldsSize);

            int fieldIndex = 0;
            for (Map.Entry<String, Schema> field : objectSchema.getProperties().entrySet()) {

                String name = field.getKey();
                Type type = schema.getType(name);
                FieldWriter writer = createWriter(field.getValue(), type);

                writer.setFieldName(name);
                writer.setIndex(fieldIndex);

                fieldWriters[fieldIndex] = writer;

                fieldIndex++;
            }

        }

        private FieldWriter CreateArrayWriter(Schema field) {

            if (field instanceof ArraySchema) {
                FieldWriter itemWriter = createWriter(((ArraySchema) field).getItems(), null);
                return new ArrayWriter(itemWriter);
            } else {
                return unknownType(field); //todo: throw a proper unknown
            }

        }

        private FieldWriter createWriter(Schema field, Type type) {

            if (field instanceof StringSchema || field instanceof PasswordSchema || field instanceof EmailSchema) {
                return new StringWriter();
                //todo: add check for: date, date-time, uuid
            } else if (field instanceof UUIDSchema) {
                //todo: fix once PARQUET-1827 is released
                return new StringWriter();
            } else if (field instanceof DateSchema) {
                return new DateWriter();
            } else if (field instanceof DateTimeSchema) {
                return new DateTimeWriter();
            } else if (field instanceof IntegerSchema) {
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

                if (field.getFormat().toLowerCase().equals("float")) {
                    return new FloatWriter();
                } else if (field.getFormat().toLowerCase().equals("double")) {
                    return new DoubleWriter();
                } else {
                    return unknownType(field);
                }

            } else if (field instanceof ArraySchema) {
                return CreateArrayWriter(field);
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

        private void writeAllFields(JsonNode pb) {

            int fieldIndex = 0;
            for (Map.Entry<String, Schema> field : objectSchema.getProperties().entrySet()) {

                fieldName = field.getKey();

                LOG.info("Looking for {}", fieldName);
                System.out.println("Looking for " + fieldName);

                if (pb.has(fieldName)) {
                    JsonNode node = pb.get(fieldName);

                    if (!node.isMissingNode()) {
                        LOG.debug("Writting field {}", fieldName);
                        fieldWriters[fieldIndex].writeField(pb.get(fieldName));
                    }
                }

                // todo: decide if we write default value instead, in which case we need to call the
                fieldIndex++; //todo: compute some index based on schema

            }
        }

    }

    class FieldWriter {
        String fieldName;
        int index = -1;

        void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        /**
         * sets index of field inside parquet message.
         */
        //todo: is this always needed?
        void setIndex(int index) {
            this.index = index;
        }

        /**
         * Used for writing repeated fields
         */
        void writeRawValue(Object value) {

        }

        void writeField(Object value) {

            if (value instanceof NullNode) {
                return;
            }

            recordConsumer.startField(fieldName, index);
            writeRawValue(value);
            recordConsumer.endField(fieldName, index);
        }

    }

    class StringWriter extends FieldWriter {
        @Override
        final void writeRawValue(Object value) {
            JsonNode node = (JsonNode) value;

            if (node.isTextual()) {
                Binary binaryString = Binary.fromString(node.asText());
                recordConsumer.addBinary(binaryString);
            } else {
                LOG.error("Type {} not expected in {}", StringWriter.class.getName(), node.getNodeType());
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

}
