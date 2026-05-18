import { beforeAll, describe, expect, it } from 'vitest';
import init, {
  ColumnOptions,
  Database,
  JsDataType,
} from '../wasm/cynos_database.js';
import { ResultSet, snapshotSchemaLayout } from '../src/result-set.js';

beforeAll(async () => {
  await init();
});

function nextMicrotask(): Promise<void> {
  return Promise.resolve();
}

describe('Binary Subscription APIs', () => {
  it('changes().subscribeBinary emits transferable snapshots that decode with a serialized layout', async () => {
    const db = new Database('binary_changes_stream');

    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('items').values([{ id: 1, name: 'Item 1' }]).exec();

    const stream = db.select('*').from('items').changes();
    const layout = snapshotSchemaLayout(stream.getSchemaLayout());
    const snapshots: Array<Array<Record<string, unknown>>> = [];

    const unsubscribe = stream.subscribeBinary((binary: any) => {
      const transferable = binary.intoTransferable();
      const hostedBytes = structuredClone(transferable, {
        transfer: [transferable.buffer],
      });
      const rows = new ResultSet(hostedBytes, layout).toArray();
      snapshots.push(rows);
    });

    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]).toEqual([{ id: 1, name: 'Item 1' }]);

    await db.insert('items').values([{ id: 2, name: 'Item 2' }]).exec();
    await nextMicrotask();

    expect(snapshots).toHaveLength(2);
    expect(snapshots[1]).toEqual([
      { id: 1, name: 'Item 1' },
      { id: 2, name: 'Item 2' },
    ]);

    unsubscribe();
  });

  it('observe().subscribeBinary emits binary updates without an eager initial push', async () => {
    const db = new Database('binary_observe_stream');

    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('users').values([{ id: 1, name: 'Alice' }]).exec();

    const observable = db.select('*').from('users').observe();
    const layout = snapshotSchemaLayout(observable.getSchemaLayout());
    const updates: Array<Array<Record<string, unknown>>> = [];

    const unsubscribe = observable.subscribeBinary((binary: any) => {
      const transferred = binary.intoTransferable();
      const rows = new ResultSet(transferred, layout).toArray();
      updates.push(rows);
    });

    expect(updates).toHaveLength(0);

    await db.insert('users').values([{ id: 2, name: 'Bob' }]).exec();
    await nextMicrotask();

    expect(updates).toHaveLength(1);
    expect(updates[0]).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);

    unsubscribe();
  });
});
