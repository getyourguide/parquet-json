# parquet-json
Apache Parquet JSON integration

## Description

This project is a spin-off of the [parquet-mr](https://github.com/apache/parquet-mr) project.
We propose to implement a converter to write JsonNode objects to parquet directly without
intermediately format. To do so, this project implements the `WriteSupport` interface for Jackson
`JsonNode` objects.

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
| string       | byte           | String    | to be improved             |
| string       | binary         | binary    | not implemented            |
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

