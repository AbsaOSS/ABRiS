package za.co.absa.avro.dataframes.avro.write

import org.scalatest.FlatSpec
import java.util.Arrays
import java.util.Collection
import scala.collection.JavaConversions._

class ScalaCustomDatumWriterSpec extends FlatSpec {

  private val writer = new ScalaCustomDatumWriter
  
  behavior of "ScalaCustomDatumWriter" 
  
  it should "retrieve the size of Java Collections" in {
    val list = new java.util.ArrayList(Arrays.asList(1))
    val set = new java.util.HashSet(Arrays.asList(1,2))
    val deque = new java.util.ArrayDeque(Arrays.asList(1,2,3))
    
    assert(writer.getArraySize(list) == list.size())
    assert(writer.getArraySize(set) == set.size())
    assert(writer.getArraySize(deque) == deque.size())
  }
  
  it should "retrieve the size os Scala Iterables" in {
    val list = List(1,2,3,4)
    val seq = Seq(1,2,3,4,5)
    val set = Set(1,2,3,4,5,6)
    val map = Map(1 -> 2, 2 -> 3)
    
    assert(writer.getArraySize(list) == list.size)
    assert(writer.getArraySize(seq) == seq.size)
    assert(writer.getArraySize(set) == set.size)
    assert(writer.getArraySize(map) == map.size)
  }
  
  it should "retrieve the size of Scala arrays" in {
    val array = Array(new Integer(1), new Integer(2), new Integer(3))
    assert(writer.getArraySize(array) == array.size)
  }
  
  it should "retrieve elements from Java Collections" in {
    val list = new java.util.ArrayList(Arrays.asList(1))
    val set = new java.util.HashSet(Arrays.asList(1,2))
    val deque = new java.util.ArrayDeque(Arrays.asList(1,2,3))    
    
    val listIt = writer.getArrayElements(list)
    assert(contains(list, listIt))
    
    val setIt = writer.getArrayElements(set)
    assert(contains(set, setIt))
    
    val dequeIt = writer.getArrayElements(deque)
    assert(contains(deque, dequeIt))    
  }
  
  it should "retrieve elements from Scala Iterables" in {
    val list = List(1,2,3,4)
    val seq = Seq(1,2,3,4,5)
    val set = Set(1,2,3,4,5,6)    
    
    val listIt = writer.getArrayElements(list)
    assert(contains(list, listIt))
    
    val setIt = writer.getArrayElements(set)
    assert(contains(set, setIt))
    
    val seqIt = writer.getArrayElements(seq)
    assert(contains(seq, seqIt))      
  }
  
  it should "retrieve elements from Scala arrays" in {
    val array = Array(new Integer(1), new Integer(2), new Integer(3))
    
    val arrayIt = writer.getArrayElements(array)
    assert(contains(array, arrayIt))
  }
  
  private def contains(collection: Collection[_ <: Any], iterator: Iterator[Object]): Boolean = {    
    if (iterator.size != collection.size()) return false
    while (iterator.hasNext) {
      if (!collection.contains(iterator.next())) {
        return false
      }
    }
    true   
  }
  
  private def contains(array: Array[_ <: Any], iterator: Iterator[Object]): Boolean = {    
    if (iterator.size != array.size) return false
    while (iterator.hasNext) {
      if (!array.contains(iterator.next())) {
        return false
      }
    }
    true   
  }  
}