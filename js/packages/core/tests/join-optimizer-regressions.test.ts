import { beforeAll, describe, expect, it } from 'vitest'
import init, {
  ColumnOptions,
  Database,
  JsDataType,
  JsSortOrder,
  col,
} from '../wasm/cynos_database.js'

beforeAll(async () => {
  await init()
})

describe('Join optimizer regressions', () => {
  it('preserves join-column table identity for WHERE and ORDER BY clauses', async () => {
    const db = new Database('join_optimizer_modifier_identity')

    const users = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .index('idx_users_name', 'name')
    db.registerTable(users)

    const orders = db.createTable('orders')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('user_id', JsDataType.Int64, null)
      .column('amount', JsDataType.Int64, null)
      .index('idx_orders_user_id', 'user_id')
      .index('idx_orders_amount', 'amount')
    db.registerTable(orders)

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Cara' },
    ]).exec()

    await db.insert('orders').values([
      { id: 10, user_id: 1, amount: 50 },
      { id: 11, user_id: 2, amount: 150 },
      { id: 12, user_id: 3, amount: 220 },
    ]).exec()

    const query = db.select(['users.id', 'orders.amount'])
      .from('users')
      .leftJoin('orders', col('users.id').eq(col('orders.user_id')))
      .where(col('orders.amount').gte(100))
      .orderBy('users.id', JsSortOrder.Asc)

    const plan = query.explain()
    const optimized = String(plan.optimized)
    const physical = String(plan.physical)
    const rows = await query.exec()

    expect(rows).toEqual([
      { id: 2, amount: 150 },
      { id: 3, amount: 220 },
    ])

    expect(optimized).toContain('join_type: Inner')
    expect(optimized).toContain('column: "amount"')
    expect(optimized).toContain('table: "orders"')
    expect(optimized).toContain('column: "id"')
    expect(optimized).toContain('table: "users"')
    expect(physical).toContain('join_type: Inner')
  })

  it('removes unused cardinality-preserving left joins from both query and trace paths', async () => {
    const db = new Database('join_optimizer_outer_join_removal')

    const users = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
    db.registerTable(users)

    const orders = db.createTable('orders')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('amount', JsDataType.Int64, null)
    db.registerTable(orders)

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]).exec()

    await db.insert('orders').values([
      { id: 1, amount: 50 },
    ]).exec()

    const query = db.select(['users.name'])
      .from('users')
      .leftJoin('orders', col('users.id').eq(col('orders.id')))

    const plan = query.explain()
    const optimized = String(plan.optimized)
    const physical = String(plan.physical)

    expect(optimized).not.toContain('table: "orders"')
    expect(physical).not.toContain('table: "orders"')

    const rows = await query.exec()
    expect(rows).toEqual([
      { name: 'Alice' },
      { name: 'Bob' },
    ])

    const observable = query.trace()
    const initialTraceResult = observable.getResult()
    expect(initialTraceResult).toHaveLength(2)

    let notifications = 0
    const unsubscribe = observable.subscribe(() => {
      notifications += 1
    })

    await db.insert('orders').values([
      { id: 2, amount: 125 },
    ]).exec()

    expect(notifications).toBe(0)
    expect(observable.getResult()).toEqual(initialTraceResult)

    unsubscribe()
  })
})
