package za.co.absa.avro.dataframes.utils.avro.data.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for nested Avro data container objects.
 * 
 * Beans annotated with this will be converted into Avro IndexedRecords during data transmission.
 * 
 * This annotation should only be used by nested classes. For example, if Class A contains instances of Class B and C, classes B and C
 * should be annotated as NestedAvroData.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface NestedAvroData {

}
