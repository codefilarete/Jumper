# Package for database metadata collect

[MetadataReader](MetadataReader.java) gives a main contract to retrieve schema elements available through JDBC DatabaseMetadata.

[DefaultMetadataReader](DefaultMetadataReader.java) is its main implementation, but one may make its own for performance issue for example.

For schema elements that are not available through JDBC DatabaseMetadata, some specialized interfaces are available, such as [SequenceMetadataReader](SequenceMetadataReader.java). Thus, implementation is database dependent and can be found in adapter modules.