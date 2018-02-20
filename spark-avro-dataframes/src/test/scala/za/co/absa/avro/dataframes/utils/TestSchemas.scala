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