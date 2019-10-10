/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.examples.data.generation

/**
 * Provides several Avro schemas.
 *
 * Used for tests and examples.
 */
object TestSchemas {

  case class ErrorMessage(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String],
                          mappings: Seq[Mapping] = Seq())

  case class Mapping(mappingTableColumn: String, mappedDatasetColumn: String)

  val NATIVE_SIMPLE_OUTER_SCHEMA = """{
    "type":"record",
    "name":"outer_nested",
    "namespace":"all-types.test",
    "fields":
    [
	    {"name":"name",  "type":"string"},
	    {"name":"nested","type":
		    {
			    "type":"record","name":"nested","fields":
				  [
					  {"name":"int", "type":"int"},
					  {"name":"long","type":"long"}
				  ]
		    }
	    }
	  ]
  }"""

  val NATIVE_SIMPLE_NESTED_SCHEMA = """{
     "namespace": "all-types.test",
     "type":"record",
      "name":"nested",
      "fields":
				  [
					  {"name":"int", "type":"int"},
					  {"name":"long","type":"long"}
					]
  }"""

  val NATIVE_COMPLETE_SCHEMA = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "native_complete",
     "fields":[
         {"name": "bytes", "type": "bytes" },
         { "name": "string",      "type": ["string", "null"], "default":"blue" },
         { "name": "int",         "type": ["int",    "null"] },
         { "name": "long",        "type": ["long",   "null"] },
 		     { "name": "double",      "type": ["double", "null"] },
 		     { "name": "float",       "type": ["float",  "null"] },
 		     { "name": "boolean",     "type": ["boolean","null"] },
 		     { "name": "array", "type": {"type": "array", "items": "string"} },
 		     {"name": "map", "type": { "type": "map", "values": {"type": "array", "items": "long"}}},
 		     {"name": "fixed",  "type": {"type": "fixed", "size": 40, "name": "fixed"}}
     ]
  }"""

  val NATIVE_COMPLETE_SCHEMA_WITHOUT_FIXED = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "native_complete",
     "fields":[
         {"name": "bytes", "type": "bytes" },
         { "name": "string",      "type": ["string", "null"] },
         { "name": "int",         "type": ["int",    "null"] },
         { "name": "long",        "type": ["long",   "null"] },
 		     { "name": "double",      "type": ["double", "null"] },
 		     { "name": "float",       "type": ["float",  "null"] },
 		     { "name": "boolean",     "type": ["boolean","null"] },
 		     { "name": "array", "type": {"type": "array", "items": "string"} },
 		     {"name": "map", "type": { "type": "map", "values": {"type": "array", "items": "long"}}}
     ]
  }"""

  val NATIVE_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "native",
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
     "name": "array",
     "fields":[
 		     { "name": "array", "type": {"type": "array", "items": "string"} }
     ]
  }"""

  val MAP_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "map",
     "fields":[
 		     {"name": "map", "type": { "type": "map", "values": {"type": "array", "items": "long"}}}
     ]
  }"""

  val BYTES_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "bytes",
     "fields":[
 		     {"name": "bytes", "type": "bytes" }
     ]
  }"""

  val FIXED_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "fixed_name",
     "fields":[
 		     {"name": "fixed", "type": {"type": "fixed", "size": 13, "name": "fixed"}}
     ]
  }"""

  val DECIMAL_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "decimal",
     "fields":[
 		     {"name": "decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}}
     ]
  }"""

  val DATE_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "date",
     "fields":[
 		     {"name": "date", "type": {"type": "int", "logicalType": "date"}}
     ]
  }"""

  val MILLISECOND_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "millisecond",
     "fields":[
 		     {"name": "millisecond", "type": {"type": "int", "logicalType": "time-millis"}}
     ]
  }"""

  val MICROSECOND_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "microsecond",
     "fields":[
 		     {"name": "microsecond", "type": {"type": "long", "logicalType": "time-micros"}}
     ]
  }"""

  val TIMESTAMP_MILLIS_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "timestamp_millis",
     "fields":[
 		     {"name": "timestampMillis", "type": {"type": "long", "logicalType": "timestamp-millis"}}
     ]
  }"""

  val TIMESTAMP_MICROS_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "timestamp_micros",
     "fields":[
 		     {"name": "timestampMicros", "type": {"type": "long", "logicalType": "timestamp-micros"}}
     ]
  }"""

  val DURATION_MICROS_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "duration_micros",
     "fields":[
 		     {"name": "duration", "type": {"type": "fixed", "size": 12, "name": "name", "logicalType": "duration"}}
     ]
  }"""

  val COMPLEX_SCHEMA_SPEC = """{
	"type":"record",
	"name":"complex",
	"namespace":"all-types.test",
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

  val CASE_CLASSES_SCHEMA = """
  {
   "type":"record",
   "name":"name",
   "namespace":"namespace",
   "fields":[
      {
         "name":"errCol",
         "type":[
            {
               "type":"array",
               "items":[
                  {
                     "type":"record",
                     "name":"errCol",
                     "namespace":"namespace.errCol",
                     "fields":[
                        {
                           "name":"errType",
                           "type":[
                              "string",
                              "null"
                           ]
                        },
                        {
                           "name":"errCode",
                           "type":[
                              "string",
                              "null"
                           ]
                        },
                        {
                           "name":"errMsg",
                           "type":[
                              "string",
                              "null"
                           ]
                        },
                        {
                           "name":"errCol",
                           "type":[
                              "string",
                              "null"
                           ]
                        },
                        {
                           "name":"rawValues",
                           "type":[
                              {
                                 "type":"array",
                                 "items":[
                                    "string",
                                    "null"
                                 ]
                              },
                              "null"
                           ]
                        },
                        {
                           "name":"mappings",
                           "type":[
                              {
                                 "type":"array",
                                 "items":[
                                    {
                                       "type":"record",
                                       "name":"mappings",
                                       "namespace":"namespace.errCol.mappings",
                                       "fields":[
                                          {
                                             "name":"mappingTableColumn",
                                             "type":[
                                                "string",
                                                "null"
                                             ]
                                          },
                                          {
                                             "name":"mappedDatasetColumn",
                                             "type":[
                                                "string",
                                                "null"
                                             ]
                                          }
                                       ]
                                    },
                                    "null"
                                 ]
                              },
                              "null"
                           ]
                        }
                     ]
                  },
                  "null"
               ]
            },
            "null"
         ]
      }
   ]
}
    """
}
