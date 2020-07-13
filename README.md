# parquet-json
Apache Parquet JSON integration

## Description

This project is a spin-off of the [parquet-mr](https://github.com/apache/parquet-mr) project.
We propose to implement a converter to write JsonNode objects to parquet directly without
intermediately format. To do so, this project implements the `WriteSupport` interface for Jackson
`JsonNode` objects, and relies on a OpenAPI based schema definition.

This project is mostly based on the ProtocolBuffer and Avro converters implementations.

## Data mapping

| OpenAPI Type | OpenAPI format | Parquet   | Comment                    |
|--------------|----------------|-----------|----------------------------|
| integer      | int16          | int16     | not a valid OAPI type      |
| integer      | int32          | int32     |                            |
| integer      | int64          | int64     |                            |
| integer      | -              | int32     | default format int32       |
| number       | float          | float     |                            |
| number       | double         | double    |                            |
| number       | -              | float     | default format float       |
| string       | -              | String    | logical type               |
| string       | password       | String    | logical type               |
| string       | email          | String    | logical type               |
| string       | UUID           | String    | to be improved             |
| string       | byte           | String    | base64 encoded bytes string|
| string       | binary         | binary    | not supported              |
| string       | date           | date      | logical type               |
| string       | date-time      | timestamp | MILLIS precision           |
| boolean      | -              | boolean   |                            |
| arrays       | -              | list      | logical type, array of maps not implemented|
| object       | -              | GroupType |                            |
| oneOf        | -              | Union     | not implemented            |
| allOf        | -              |           | not supported              |
| map          | -              | map       | keys as string only, "free form" objects and "Fixed Keys" not supported         |
| enum         | -              | enum      | only string type supported |

## How to use the converter

Given for example a schema definition in a file `openapi.yaml` as:

```yaml
openapi: 3.0.1
info:
  title: Some schemas
  description: Some schemas for parquet-json usage example
  version: 1.0.0
servers:
  - url: 'https://getyourguide.com'
paths: {}
components:
  schemas:
    MyObject:
      title: MyObject
      type: object
      properties:
        key_string:
          type: string
          nullable: false
          default: 'a string'
        key_int32:
          type: integer
          format: int32
          nullable: true
          default: 1
        is_true:
          type: boolean
          nullable: true
          default: true
```

the converter can be used to write a parquet file on the local FS with:

```java
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    OpenAPI openAPI = new OpenAPIV3Parser().read("openapi.yaml");

    ObjectSchema schema = (ObjectSchema) openAPI.getComponents().getSchemas().get("MyObject");

    ObjectMapper mapper = new ObjectMapper();

    String output = "./example.parquet";
    Path path = new Path(output);

    ParquetWriter<JsonNode> writer =
        JsonParquetWriter.Builder(path)
            .withSchema(schema)
            .withConf(conf)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withDictionaryEncoding(true)
            .withPageSize(1024 * 1024)
            .build();

    String json =
        "{\"key_string\":\"hello\",\"key_int32\":32,\"is_true\":true}";
    JsonNode payload = mapper.readTree(json);
    
    writer.write(payload);
    
    writer.close();
```

## Known limitations

- Currently works only with schemas of type `OpenAPI` (https://github.com/swagger-api/swagger-parser/) and data payload of type `JsonNode` (Jackson library).
- The schema must be fully resolved (no internal or external `ref`)
- Union types (`oneOf`) not implemented yet
- Readers (from Parquet to JsonNode/OpenAPI) are not implemented (we don't need this part here at GetYourGuide)

## Contributing

We welcome pull requests; if you are planning to perform bigger changes then it makes sense to file an issue first.

## Security
For sensitive security matters please contact [security@getyourguide.com](mailto:security@getyourguide.com).

## Legal
Copyright 2020 GetYourGuide GmbH.

`parquet-json` is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
