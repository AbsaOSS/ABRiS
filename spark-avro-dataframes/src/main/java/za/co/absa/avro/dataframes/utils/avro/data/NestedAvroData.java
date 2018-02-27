package za.co.absa.avro.dataframes.utils.avro.data;

/**
 * Markup interface for nested Avro data container objects.
 * 
 * Beans marked with this interface will be converted into Avro IndexedRecords during data transmission.
 * 
 * This interface should only be used by nested classes. For example, if Class A contains instances of Class B and C, classes B and C
 * should be marked as NestedAvroData whereas class A should be marked as {@link ContainerAvroData}.
 */
public interface NestedAvroData {

}
