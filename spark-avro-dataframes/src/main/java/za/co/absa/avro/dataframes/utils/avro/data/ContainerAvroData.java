package za.co.absa.avro.dataframes.utils.avro.data;

/**
 * Markup interface for container objects. 
 * This interface should be implemented by the most outer Avro container class.
 * 
 * For example, if Class A contains instances of Class B and C, classes B and C
 * should be marked as {@link NestedAvroData} whereas class A should be marked as ContainerAvroData.
 * 
 */
public interface ContainerAvroData {

}
