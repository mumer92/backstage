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

import { ConflictError } from '@backstage/backend-common';
import Knex from 'knex';
import path from 'path';
import { Database } from './Database';
import {
  AddDatabaseLocation,
  DbEntityRequest,
  DbEntityResponse,
  DbLocationsRow,
} from './types';

describe('Database', () => {
  let database: Knex;
  let entityRequest: DbEntityRequest;
  let entityResponse: DbEntityResponse;

  beforeEach(async () => {
    database = Knex({
      client: 'sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
    });

    await database.raw('PRAGMA foreign_keys = ON');
    await database.migrate.latest({
      directory: path.resolve(__dirname, 'migrations'),
      loadExtensions: ['.ts'],
    });

    entityRequest = {
      entity: {
        apiVersion: 'a',
        kind: 'b',
        metadata: {
          name: 'c',
          namespace: 'd',
          labels: { e: 'f' },
          annotations: { g: 'h' },
        },
        spec: { i: 'j' },
      },
    };

    entityResponse = {
      locationId: undefined,
      entity: {
        apiVersion: 'a',
        kind: 'b',
        metadata: {
          uid: expect.anything(),
          generation: expect.anything(),
          name: 'c',
          namespace: 'd',
          labels: { e: 'f' },
          annotations: { g: 'h' },
        },
        spec: { i: 'j' },
      },
    };
  });

  it('manages locations', async () => {
    const db = new Database(database);
    const input: AddDatabaseLocation = { type: 'a', target: 'b' };
    const output: DbLocationsRow = {
      id: expect.anything(),
      type: 'a',
      target: 'b',
    };

    await db.addLocation(input);

    const locations = await db.locations();
    expect(locations).toEqual([output]);
    const location = await db.location(locations[0].id);
    expect(location).toEqual(output);

    await db.removeLocation(locations[0].id);

    await expect(db.locations()).resolves.toEqual([]);
    await expect(db.location(locations[0].id)).rejects.toThrow(
      /Found no location/,
    );
  });

  it('instead of adding second location with the same target, returns existing one', async () => {
    // Prepare
    const catalog = new Database(database);
    const input: AddDatabaseLocation = { type: 'a', target: 'b' };
    const output1: DbLocationsRow = await catalog.addLocation(input);

    // Try to insert the same location
    const output2: DbLocationsRow = await catalog.addLocation(input);
    const locations = await catalog.locations();

    // Output is the same
    expect(output2).toEqual(output1);
    // Locations contain only one record
    expect(locations).toEqual([output1]);
  });

  describe('addEntity', () => {
    it('happy path: adds entity to empty database', async () => {
      const catalog = new Database(database);
      await expect(
        catalog.transaction(tx => catalog.addEntity(tx, entityRequest)),
      ).resolves.toStrictEqual(entityResponse);
    });

    it('rejects adding the same-named entity twice', async () => {
      const catalog = new Database(database);
      await catalog.transaction(tx => catalog.addEntity(tx, entityRequest));
      await expect(
        catalog.transaction(tx => catalog.addEntity(tx, entityRequest)),
      ).rejects.toThrow(ConflictError);
    });

    it('accepts adding the same-named entity twice if on different namespaces', async () => {
      const catalog = new Database(database);
      entityRequest.entity.metadata!.namespace = 'namespace1';
      await catalog.transaction(tx => catalog.addEntity(tx, entityRequest));
      entityRequest.entity.metadata!.namespace = 'namespace2';
      await expect(
        catalog.transaction(tx => catalog.addEntity(tx, entityRequest)),
      ).resolves.toBeDefined();
    });
  });

  /*
  describe('addOrUpdateEntity', () => {
    it('happy path: adds entity to empty database', async () => {
      const catalog = new Database(database);
      await expect(
        catalog.addOrUpdateEntity(entityRequest),
      ).resolves.toStrictEqual(entityResponse);
    });

    it('accepts update with uid and generation', async () => {
      const catalog = new Database(database);
      const added = await catalog.addOrUpdateEntity(entityRequest);

      added.entity.kind = 'new';
      const updated = await catalog.addOrUpdateEntity(added);
      expect(updated.entity.metadata!.generation).toEqual(
        added.entity.metadata!.generation! + 1,
      );
      expect(updated.entity.kind).toBe('new');

      const read = await catalog.entity(
        added.entity.metadata!.name!,
        added.entity.metadata!.namespace,
      );
      expect(read.entity.metadata!.generation).toEqual(
        added.entity.metadata!.generation! + 1,
      );
      expect(read.entity.kind).toBe('new');
    });

    it('rejects update with uid and wrong generation', async () => {
      const catalog = new Database(database);
      const added = await catalog.addOrUpdateEntity(entityRequest);

      added.entity.kind = 'new';
      added.entity.metadata!.generation! += 7;
      await expect(catalog.addOrUpdateEntity(added)).rejects.toThrow(
        /entity matching/i,
      );
    });

    it('accepts update with uid without generation', async () => {
      const catalog = new Database(database);
      const added = await catalog.addOrUpdateEntity(entityRequest);
      const oldGeneration = added.entity.metadata!.generation!;

      added.entity.kind = 'new';
      added.entity.metadata!.generation = undefined;

      const updated = await catalog.addOrUpdateEntity(added);
      expect(updated.entity.metadata!.generation).toEqual(oldGeneration + 1);
      expect(updated.entity.kind).toBe('new');

      const read = await catalog.entity(
        added.entity.metadata!.name!,
        added.entity.metadata!.namespace,
      );
      expect(read.entity.metadata!.generation).toEqual(oldGeneration + 1);
      expect(read.entity.kind).toBe('new');
    });

    it('rejects adding conflicting name', async () => {
      const catalog = new Database(database);
      await catalog.addOrUpdateEntity(entityRequest);
      await expect(catalog.addOrUpdateEntity(entityRequest)).rejects.toThrow(
        ConflictError,
      );
    });
  });
  */
});
