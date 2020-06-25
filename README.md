# parquet-json
Apache Parquet JSON integration

## Description

This project is a spin-off of the [parquet-mr](https://github.com/apache/parquet-mr) project.
We propose to implement a converter to write JsonNode objects to parquet directly without
intermediately format. To do so, this project implements the `WriteSupport` interface for Jackson
JsonNode.

This is mostly based on the ProtocolBuffer and Avro converters.

## Changelog

- Support simple String

## Todo

- [ ] primitive types
- [ ] Arrays
- [ ] Objects
- [ ] Nullable
- [ ] OpenAPI schema validation
