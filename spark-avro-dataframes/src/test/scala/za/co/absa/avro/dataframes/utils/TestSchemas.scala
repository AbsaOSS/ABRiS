package za.co.absa.avro.dataframes.utils

object TestSchemas {
  
  val NATIVE_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
         { "name": "string",      "type": ["string", "null"] },     
         { "name": "int",         "type": ["int",    "null"] },
         { "name": "long",        "type": ["long",   "null"] },
 		     { "name": "double",      "type": ["double", "null"] },
 		     { "name": "float",       "type": ["float",  "null"] },
 		     { "name": "boolean",     "type": ["boolean","null"] }
     ]
  }"""  
  
  val ARRAY_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     { "name": "array", "type": {"type": "array", "items": "string"} }
     ]
  }"""    

  val MAP_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     { name": "map", type": {{ "type": {"type": "array", "items": "long"}} },
     ]
  }"""      
  
  val BYTES_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "bytes", "type": "bytes" }
     ]
  }"""     
  
  val FIXED_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "fixed", "type": {"type": "fixed", "size": 13, "name": "fixed"}}
     ]
  }"""  
  
  val DECIMAL_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}}
     ]
  }"""    
  
  val DATE_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "date", "type": {"type": "int", "logicalType": "date"}}
     ]
  }"""   
  
  val MILLISECOND_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "millisecond", "type": {"type": "int", "logicalType": "time-millis"}}
     ]
  }"""    
  
  val MICROSECOND_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "microsecond", "type": {"type": "long", "logicalType": "time-micros"}}
     ]
  }"""     
  
  val TIMESTAMP_MILLIS_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "timestampMillis", "type": {"type": "long", "logicalType": "timestamp-millis"}}
     ]
  }"""   
  
  val TIMESTAMP_MICROS_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "timestampMicros", "type": {"type": "long", "logicalType": "timestamp-micros"}}
     ]
  }"""   
  
  val DURATION_MICROS_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
 		     {"name": "duration", "type": {"type": "fixed", "size": 12, "name": "name", "logicalType": "duration"}}
     ]
  }"""     
  
  val COMPLEX_SCHEMA_SPEC = """{	
	"type":"record",
	"name":"State",
	"namespace":"za.co.absa.avro.dataframes.parsing.generation",
	"fields":
	[
		{"name":"name","type":"string"},
		{"name":"regions","type":
							{"type":"map","values":
								{"type":"array","items":
											{"type":"record","name":"City","fields":
													[
															{"name":"name","type":"string"},
															{"name":"neighborhoods","type":
																	{"type":"array","items":
																			{"type":"record","name":"Neighborhood","fields":
																						[
																						{"name":"name","type":"string"},
																						{"name":"streets","type":
																									{"type":"array","items":
																											{"type":"record","name":"Street","fields":
																															[
																															{"name":"name","type":"string"},
																															{"name":"zip","type":"string"}
																															]
																											}
																									}
																						}
																						]
																			}
																	}
															}
													]
											}
								}
							}
		}
	]
  }"""
  
  val COMPLEX_SCHEMA_STREET_SPEC = """		
	  {
	    "namespace":"test_city",
	    "type":"record",
	    "name":"Street",
	    "fields":
  		[
	  	  {"name":"name","type":"string"},
		  	{"name":"zip","type":"string"}
		  ]		
    }"""  
  
  val COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC = """
  { 
    "namespace":"test_neighborhood",
    "type":"record",
    "name":"Neighborhood",
    "fields":
		  [
			  {"name":"name","type":"string"},
				{"name":"streets",
				  "type":
			    {
			      "type":"array",
			      "items":
				    {
				      "type":"record",
				      "name":"Street",
				      "fields":
						    [
								  {"name":"name","type":"string"},
									{"name":"zip","type":"string"}
								]
					  }
				  }
	      }
			]
	}"""
  
  val COMPLEX_SCHEMA_CITY_SPEC = """
    {	
      "namespace":"test_city",
      "type":"record",
      "name":"City",
      "fields":
			  [
				  {"name":"name","type":"string"},
					{"name":"neighborhoods","type":
																	{
																	  "type":"array",
																	  "items":
																		  {
																		    "type":"record",
																		    "name":"Neighborhood",
																		    "fields":
																				  [
																						{"name":"name","type":"string"},
																						{"name":"streets","type":
																									{
																									  "type":"array",
																									  "items":
																										  {
																										    "type":"record",
																										    "name":"Street",
																										    "fields":
																												  [
																													  {"name":"name","type":"string"},
																														{"name":"zip","type":"string"}
																													]
																											}
																									}
																						}
																					]
																			}
																	}
					}
				]
		}"""  
}