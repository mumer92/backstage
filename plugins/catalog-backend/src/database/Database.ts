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

import {
  ConflictError,
  InputError,
  NotFoundError,
} from '@backstage/backend-common';
import Knex from 'knex';
import { v4 as uuidv4 } from 'uuid';
import { DescriptorEnvelope } from '../ingestion';
import { buildEntitySearch } from './search';
import {
  AddDatabaseLocation,
  DatabaseLocationUpdateLogEvent,
  DatabaseLocationUpdateLogStatus,
  DbEntitiesRow,
  DbEntityRequest,
  DbEntityResponse,
  DbLocationsRow,
} from './types';

function serializeMetadata(
  metadata: DescriptorEnvelope['metadata'],
): DbEntitiesRow['metadata'] {
  if (!metadata) {
    return null;
  }

  const output = { ...metadata };
  delete output.uid;
  delete output.etag;
  delete output.generation;

  return JSON.stringify(output);
}

function serializeSpec(
  spec: DescriptorEnvelope['spec'],
): DbEntitiesRow['spec'] {
  if (!spec) {
    return null;
  }

  return JSON.stringify(spec);
}

function entityRequestToDb(request: DbEntityRequest): DbEntitiesRow {
  return {
    id: '',
    location_id: request.locationId || null,
    etag: new Buffer(uuidv4()).toString('base64').replace(/[^\w]/g, ''), // TODO(freben): Atomicity isn't checked using these yet
    generation: 1, // TODO(freben): These aren't updated yet
    api_version: request.entity.apiVersion,
    kind: request.entity.kind,
    name: request.entity.metadata?.name || null,
    namespace: request.entity.metadata?.namespace || null,
    metadata: serializeMetadata(request.entity.metadata),
    spec: serializeSpec(request.entity.spec),
  };
}

function entityDbToResponse(row: DbEntitiesRow): DbEntityResponse {
  const entity: DescriptorEnvelope = {
    apiVersion: row.api_version,
    kind: row.kind,
    metadata: {
      uid: row.id,
      etag: row.etag,
      generation: row.generation,
    },
  };

  if (row.metadata) {
    const metadata = JSON.parse(row.metadata) as DescriptorEnvelope['metadata'];
    entity.metadata = { ...entity.metadata, ...metadata };
  }

  if (row.spec) {
    const spec = JSON.parse(row.spec);
    entity.spec = spec;
  }

  return {
    locationId: row.location_id || undefined,
    entity,
  };
}

export class Database {
  constructor(private readonly database: Knex) {}

  async transaction<T>(
    fn: (tx: Knex.Transaction<any, any>) => Promise<T>,
  ): Promise<T> {
    try {
      return await this.database.transaction<T>(fn);
    } catch (e) {
      if (
        /SQLITE_CONSTRAINT: UNIQUE/.test(e.message) ||
        /unique constraint/.test(e.message)
      ) {
        throw new ConflictError(`Rejected due to a conflicting entity`, e);
      }

      throw e;
    }
  }

  async addEntity(
    tx: Knex.Transaction<any, any>,
    request: DbEntityRequest,
  ): Promise<DbEntityResponse> {
    const newRow = entityRequestToDb(request);
    newRow.id = uuidv4();
    newRow.generation = 1;

    await tx('entities').insert(newRow);

    const result = await tx<DbEntitiesRow>('entities')
      .where({ id: newRow.id })
      .select();

    if (!result.length) {
      throw new ConflictError(`Failed to read the generated entity`);
    }

    return entityDbToResponse(result[0]);
  }

  async updateEntity(
    tx: Knex.Transaction<any, any>,
    request: DbEntityRequest,
  ): Promise<DbEntityResponse> {
    const newRow = entityRequestToDb(request);

    const { uid, generation, name, namespace } = request.entity.metadata ?? {};

    // Update by uid, with generation check
    if (uid && generation) {
      const rows = await tx<DbEntitiesRow>('entities')
        .where({ id: uid, generation })
        .update({
          ...newRow,
          id: undefined,
          generation: generation + 1,
        });

      if (!rows) {
        throw new ConflictError(
          `No entity matching uid="${uid}", generation=${generation}`,
        );
      }

      return entityDbToResponse(newRow);
    }

    // Update by uid, unconditionally
    if (uid) {
      const rows = await tx<DbEntitiesRow>('entities')
        .where({ id: uid })
        .update({
          ...newRow,
          id: undefined,
          generation: tx.raw('generation + 1'),
        });

      if (!rows) {
        throw new ConflictError(`No entity matching uid="${uid}"`);
      }

      const result = await tx<DbEntitiesRow>('entities')
        .where({ id: uid })
        .select();

      if (!result.length) {
        throw new ConflictError(`Failed to read the generated entity`);
      }

      return entityDbToResponse(result[0]);
    }

    // Update by name, with generation check
    if (name && generation) {
      const rows = await tx<DbEntitiesRow>('entities')
        .where({ name, namespace: namespace || null, generation })
        .update({
          ...newRow,
          id: undefined,
          generation: generation + 1,
          name: undefined,
          namespace: undefined,
        });

      if (!rows) {
        throw new ConflictError(
          `No entity matching name="${name}" namespace=${
            namespace ? `"${namespace}"` : 'null'
          } generation ${generation}`,
        );
      }

      const result = await tx<DbEntitiesRow>('entities')
        .where({
          name,
          namespace: namespace || null,
          generation: generation + 1,
        })
        .select();

      if (!result.length) {
        throw new ConflictError(`Failed to read the generated entity`);
      }

      return entityDbToResponse(result[0]);
    }

    // Update by name, unconditionally
    if (name) {
      const oldRows = await tx<DbEntitiesRow>('entities')
        .where({ name, namespace: namespace || null })
        .select();

      if (!oldRows.length) {
        throw new ConflictError(
          `No entity matching name="${name}" namespace=${
            namespace ? `"${namespace}"` : 'null'
          }`,
        );
      }

      const rows = await tx<DbEntitiesRow>('entities')
        .where({ id: oldRows[0].id, generation: oldRows[0].generation })
        .update({
          ...newRow,
          id: undefined,
          generation: oldRows[0].generation + 1,
          name: undefined,
          namespace: undefined,
        });

      if (!rows) {
        throw new ConflictError(
          `Failed to update name="${name}" namespace=${
            namespace ? `"${namespace}"` : 'null'
          }`,
        );
      }

      const result = await tx<DbEntitiesRow>('entities')
        .where({ id: oldRows[0].id, generation: oldRows[0].generation + 1 })
        .select();

      if (!result.length) {
        throw new ConflictError(`Failed to read the generated entity`);
      }

      return entityDbToResponse(result[0]);
    }

    throw new InputError(`Cannot update entity that has neither uid nor name`);
  }

  async entities(tx: Knex.Transaction<any, any>): Promise<DbEntityResponse[]> {
    const rows = await tx<DbEntitiesRow>('entities')
      .orderBy('namespace', 'name')
      .select();
    return rows.map(row => entityDbToResponse(row));
  }

  async entity(
    tx: Knex.Transaction<any, any>,
    name: string,
    namespace?: string,
  ): Promise<DbEntityResponse | undefined> {
    const rows = await tx<DbEntitiesRow>('entities')
      .where({ name, namespace: namespace || null })
      .select();

    if (rows.length !== 1) {
      return undefined;
    }

    return entityDbToResponse(rows[0]);
  }

  async addLocation(location: AddDatabaseLocation): Promise<DbLocationsRow> {
    return await this.database.transaction<DbLocationsRow>(async tx => {
      const existingLocation = await tx<DbLocationsRow>('locations')
        .where({
          target: location.target,
        })
        .select();

      if (existingLocation?.[0]) {
        return existingLocation[0];
      }

      const id = uuidv4();
      const { type, target } = location;
      await tx<DbLocationsRow>('locations').insert({
        id,
        type,
        target,
      });

      return (await tx<DbLocationsRow>('locations').where({ id }).select())![0];
    });
  }

  async removeLocation(id: string): Promise<void> {
    const result = await this.database<DbLocationsRow>('locations')
      .where({ id })
      .del();

    if (!result) {
      throw new NotFoundError(`Found no location with ID ${id}`);
    }
  }

  async location(id: string): Promise<DbLocationsRow> {
    const items = await this.database<DbLocationsRow>('locations')
      .where({ id })
      .select();
    if (!items.length) {
      throw new NotFoundError(`Found no location with ID ${id}`);
    }
    return items[0];
  }

  async locations(): Promise<DbLocationsRow[]> {
    return this.database<DbLocationsRow>('locations').select();
  }

  async addLocationUpdateLogEvent(
    locationId: string,
    status: DatabaseLocationUpdateLogStatus,
    entityName?: string,
    message?: string,
  ): Promise<void> {
    return this.database<DatabaseLocationUpdateLogEvent>(
      'location_update_log',
    ).insert({
      id: uuidv4(),
      status: status,
      location_id: locationId,
      entity_name: entityName,
      message,
    });
  }

  /*
  private async updateEntitiesSearch(
    tx: Knex.Transaction<any, any>,
    entityId: string,
    data: DescriptorEnvelope,
  ): Promise<void> {
    const entries = buildEntitySearch(entityId, data);
    await tx('entities_search').where({ entity_id: entityId }).del();
    await tx('entities_search').insert(entries);
  }
  */
}
