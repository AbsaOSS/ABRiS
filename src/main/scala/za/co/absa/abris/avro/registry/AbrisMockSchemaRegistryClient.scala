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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaMetadata}

import java.io.IOException


class AbrisMockSchemaRegistryClient extends MockSchemaRegistryClient {

  /**
   * MockSchemaRegistryClient is throwing different Exception than the mocked client, this is a workaround
   */
  @throws[IOException]
  @throws[RestClientException]
  override def getLatestSchemaMetadata(subject: String): SchemaMetadata = {
    try (super.getLatestSchemaMetadata(subject))
    catch {
      case e: IOException if e.getMessage == "No schema registered under subject!"
        => throw new RestClientException("No schema registered under subject!", 404, 40401)
    }
  }
}
