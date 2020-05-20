/*
 * Copyright 2020 Spotify AB
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

/**
 * The format envelope that's common to all versions/kinds.
 *
 * @see https://kubernetes.io/docs/concepts/overview/working-with-objects/kubernetes-objects/
 */
export type DescriptorEnvelope = {
  /**
   * The version of specification format for this particular entity that
   * this is written against.
   */
  apiVersion: string;

  /**
   * The high level entity type being described.
   */
  kind: string;

  /**
   * Optional metadata related to the entity.
   *
   * @see https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#objectmeta-v1-meta
   */
  metadata?: {
    /**
     * A globally unique ID for the entity. This field can not be set by the
     * user at creation time, and the server will reject an attempt to do so.
     * The field will be populated in read operations. The field can
     * (optionally) be specified when performing update or delete operations,
     * but the server is free to reject requests that do so in such a way that
     * it breaks semantics.
     */
    uid?: string;

    /**
     * A positive nonzero number that indicates the current generation of data
     * for this entity; the value is incremented for each update operation.
     * This field can not be set by the user at creation time, and the server
     * will reject an attempt to do so. The field will be populated in read
     * operations. The field can (optionally) be specified when performing
     * update or delete operations, and the server will then reject the
     * operation if it does not match the current stored value.
     */
    generation?: number;

    /**
     * The name of the entity. Must be uniqe within the service at any point
     * in time, for any given namespace.
     */
    name?: string;

    /**
     * The namespace that the entity belongs to.
     */
    namespace?: string;

    /**
     * Key/value pairs of identifying information attached to the entity.
     */
    labels?: object;

    /**
     * Key/value pairs of non-identifying auxiliary information attached to the
     * entity.
     */
    annotations?: object;
  };

  /**
   * The specification data describing the entity itself.
   */
  spec?: object;
};

/**
 * Parses and validates a single envelope into its materialized kind.
 *
 * These parsers may assume that the envelope is already validated and well
 * formed.
 */
export type KindParser = {
  /**
   * Try to parse an envelope into a materialized kind.
   *
   * @param envelope A valid descriptor envelope
   * @returns A materialized type, or undefined if the given version/kind is
   *          not meant to be handled by this parser
   * @throws An Error if the type was handled and found to not be properly
   *         formatted
   */
  tryParse(
    envelope: DescriptorEnvelope,
  ): Promise<DescriptorEnvelope | undefined>;
};
