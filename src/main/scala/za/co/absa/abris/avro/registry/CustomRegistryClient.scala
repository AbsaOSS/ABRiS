/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.abris.avro.registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

/**
 * This custom registry client can be used as a super class for custom registry clients that differ from the standard
 * Confluent [[io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient]].
 * These must implement a constructor that takes a map as input that is the one configured in the AbrisConfig.
 * The class is constructed in the Spark driver and the Spark executor.
 */
abstract class CustomRegistryClient(config: java.util.Map[String, String]) extends SchemaRegistryClient {

}
