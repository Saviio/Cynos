import { beforeAll, describe, expect, it } from 'vitest';
import init, {
  ColumnOptions,
  Database,
  ForeignKeyOptions,
  JsDataType,
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

let dbCounter = 0;

function createGraphqlDb(): Database {
  dbCounter += 1;

  const db = new Database(`graphql_${dbCounter}`);
  const users = db
    .createTable('users')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null);
  db.registerTable(users);

  const posts = db
    .createTable('posts')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('author_id', JsDataType.Int64, null)
    .column('title', JsDataType.String, null)
    .foreignKey(
      'fk_posts_author',
      'author_id',
      'users',
      'id',
      new ForeignKeyOptions().fieldName('author').reverseFieldName('posts')
    );
  db.registerTable(posts);

  return db;
}

function createGraphqlUniqueProfileDb(): Database {
  const db = createGraphqlDb();

  const profiles = db
    .createTable('profiles')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('user_id', JsDataType.Int64, new ColumnOptions().setUnique(true))
    .column('bio', JsDataType.String, null)
    .foreignKey(
      'fk_profiles_user',
      'user_id',
      'users',
      'id',
      new ForeignKeyOptions().fieldName('user').reverseFieldName('profile')
    );
  db.registerTable(profiles);

  return db;
}

async function flushGraphqlReactivity(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await Promise.resolve();
}

function dataOf<T>(payload: { data: T }): T {
  return payload.data;
}

function sortById<T extends { id: number }>(rows: T[]): T[] {
  return [...rows].sort((left, right) => left.id - right.id);
}

describe('GraphQL', () => {
  it('exposes schema, root planner filters, and nested relations', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]).exec();
    await db.insert('posts').values([
      { id: 10, author_id: 1, title: 'Hello' },
      { id: 11, author_id: 1, title: 'World' },
      { id: 12, author_id: 2, title: 'Other' },
    ]).exec();

    const sdl = db.graphqlSchema();
    expect(sdl).toContain('type Query');
    expect(sdl).toContain('type Mutation');
    expect(sdl).toContain('type Subscription');
    expect(sdl).toContain('insertUsers');
    expect(sdl).toContain('posts(');
    expect(sdl).toContain('author: Users');

    const result = db.graphql(`
      {
        users(
          where: { id: { eq: 1 } }
          orderBy: [{ field: ID, direction: ASC }]
        ) {
          id
          name
          posts(orderBy: [{ field: ID, direction: ASC }]) {
            id
            title
          }
        }
      }
    `);

    expect(dataOf(result)).toEqual({
      users: [
        {
          id: 1,
          name: 'Alice',
          posts: [
            { id: 10, title: 'Hello' },
            { id: 11, title: 'World' },
          ],
        },
      ],
    });
  });

  it('supports mutation-driven subscriptions end to end', async () => {
    const db = createGraphqlDb();
    const subscription = db.subscribeGraphql(`
      subscription {
        users(orderBy: [{ field: ID, direction: ASC }]) {
          id
          name
        }
      }
    `);

    expect(dataOf(subscription.getResult())).toEqual({ users: [] });

    const seen: Array<{ data: { users: Array<{ id: number; name: string }> } }> = [];
    const unsubscribe = subscription.subscribe((payload) => {
      seen.push(payload);
    });
    expect(subscription.subscriptionCount()).toBe(1);

    const inserted = db.graphql(`
      mutation {
        insertUsers(input: [{ id: 1, name: "Alice" }]) {
          id
          name
        }
      }
    `);
    expect(dataOf(inserted)).toEqual({
      insertUsers: [{ id: 1, name: 'Alice' }],
    });

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice' }],
    });

    const updated = db.graphql(`
      mutation {
        updateUsers(where: { id: { eq: 1 } }, set: { name: "Alicia" }) {
          id
          name
        }
      }
    `);
    expect(dataOf(updated)).toEqual({
      updateUsers: [{ id: 1, name: 'Alicia' }],
    });

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alicia' }],
    });
    expect(seen.at(-1)).toEqual({
      data: { users: [{ id: 1, name: 'Alicia' }] },
    });

    unsubscribe();
    expect(subscription.subscriptionCount()).toBe(0);
  });

  it('uses the unified delta-backed live plan for scalar root subscriptions', async () => {
    const db = createGraphqlDb();
    const subscription = db.subscribeGraphql(`
      subscription UserCard {
        usersByPk(pk: { id: 1 }) {
          id
          name
        }
      }
    `);

    expect(dataOf(subscription.getResult())).toEqual({ usersByPk: null });

    db.graphql(`
      mutation {
        insertUsers(input: [{ id: 1, name: "Alice" }]) {
          id
          name
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      usersByPk: { id: 1, name: 'Alice' },
    });

    db.graphql(`
      mutation {
        updateUsers(where: { id: { eq: 1 } }, set: { name: "Alicia" }) {
          id
          name
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      usersByPk: { id: 1, name: 'Alicia' },
    });
  });

  it('uses the delta-backed live plan for multi-level nested subscriptions without sorting', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 2, name: 'Bob' },
    ]).exec();
    await db.insert('posts').values([
      { id: 10, author_id: 2, title: 'First' },
    ]).exec();

    const subscription = db.subscribeGraphql(`
      subscription PostAuthorGraph {
        postsByPk(pk: { id: 10 }) {
          id
          title
          author {
            id
            name
            posts {
              id
              title
            }
          }
        }
      }
    `);

    expect(dataOf(subscription.getResult())).toEqual({
      postsByPk: {
        id: 10,
        title: 'First',
        author: {
          id: 2,
          name: 'Bob',
          posts: [{ id: 10, title: 'First' }],
        },
      },
    });

    const seen: Array<{
      data: {
        postsByPk: {
          id: number;
          title: string;
          author: {
            id: number;
            name: string;
            posts: Array<{ id: number; title: string }>;
          };
        };
      };
    }> = [];
    const unsubscribe = subscription.subscribe((payload) => {
      seen.push(payload);
    });

    db.graphql(`
      mutation {
        insertPosts(input: [{ id: 11, author_id: 2, title: "Second" }]) {
          id
        }
      }
    `);

    await flushGraphqlReactivity();

    const current = dataOf(subscription.getResult());
    expect(current.postsByPk.id).toBe(10);
    expect(current.postsByPk.author.name).toBe('Bob');
    expect(sortById(current.postsByPk.author.posts)).toEqual([
      { id: 10, title: 'First' },
      { id: 11, title: 'Second' },
    ]);
    expect(sortById(dataOf(seen.at(-1)!).postsByPk.author.posts)).toEqual([
      { id: 10, title: 'First' },
      { id: 11, title: 'Second' },
    ]);

    unsubscribe();
  });

  it('keeps unique reverse relation limit(1) subscriptions live without snapshot fallback semantics', async () => {
    const db = createGraphqlUniqueProfileDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
    ]).exec();

    const subscription = db.subscribeGraphql(`
      subscription UserCard {
        usersByPk(pk: { id: 1 }) {
          id
          name
          profile(limit: 1) {
            id
            bio
          }
        }
      }
    `);

    expect(dataOf(subscription.getResult())).toEqual({
      usersByPk: {
        id: 1,
        name: 'Alice',
        profile: [],
      },
    });

    db.graphql(`
      mutation {
        insertProfiles(input: [{ id: 10, user_id: 1, bio: "First" }]) {
          id
          bio
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      usersByPk: {
        id: 1,
        name: 'Alice',
        profile: [{ id: 10, bio: 'First' }],
      },
    });

    db.graphql(`
      mutation {
        updateProfiles(where: { id: { eq: 10 } }, set: { bio: "Updated" }) {
          id
          bio
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      usersByPk: {
        id: 1,
        name: 'Alice',
        profile: [{ id: 10, bio: 'Updated' }],
      },
    });
  });

  it('pushes nested relation updates for GraphQL subscriptions', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
    ]).exec();

    const subscription = db.subscribeGraphql(`
      subscription WatchUsersWithPosts {
        users(orderBy: [{ field: ID, direction: ASC }]) {
          id
          name
          posts(orderBy: [{ field: ID, direction: ASC }]) {
            id
            title
          }
        }
      }
    `);

    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [] }],
    });

    const seen: Array<{
      data: {
        users: Array<{
          id: number;
          name: string;
          posts: Array<{ id: number; title: string }>;
        }>;
      };
    }> = [];
    const unsubscribe = subscription.subscribe((payload) => {
      seen.push(payload);
    });

    expect(dataOf(seen[0]!)).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [] }],
    });

    db.graphql(`
      mutation {
        insertPosts(input: [{ id: 10, author_id: 1, title: "Hello" }]) {
          id
          title
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [{ id: 10, title: 'Hello' }] }],
    });
    expect(dataOf(seen.at(-1)!)).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [{ id: 10, title: 'Hello' }] }],
    });

    db.graphql(`
      mutation {
        updatePosts(where: { id: { eq: 10 } }, set: { title: "Updated" }) {
          id
          title
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [{ id: 10, title: 'Updated' }] }],
    });
    expect(dataOf(seen.at(-1)!)).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [{ id: 10, title: 'Updated' }] }],
    });

    unsubscribe();
  });

  it('keeps getResult current even without external GraphQL subscribers', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
    ]).exec();

    const subscription = db.subscribeGraphql(`
      subscription WatchUsersWithPosts {
        users(orderBy: [{ field: ID, direction: ASC }]) {
          id
          name
          posts(orderBy: [{ field: ID, direction: ASC }]) {
            id
            title
          }
        }
      }
    `);

    expect(subscription.subscriptionCount()).toBe(0);
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [] }],
    });

    db.graphql(`
      mutation {
        insertPosts(input: [{ id: 10, author_id: 1, title: "Hello" }]) {
          id
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice', posts: [{ id: 10, title: 'Hello' }] }],
    });

    db.graphql(`
      mutation {
        updateUsers(where: { id: { eq: 1 } }, set: { name: "Alicia" }) {
          id
        }
      }
    `);

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alicia', posts: [{ id: 10, title: 'Hello' }] }],
    });
  });

  it('supports prepared GraphQL mutation and subscription documents', async () => {
    const db = createGraphqlDb();
    const preparedSubscription = db.prepareGraphql(
      `
        subscription UserFeed {
          users(orderBy: [{ field: ID, direction: ASC }]) {
            id
            name
          }
        }
      `,
      'UserFeed'
    );
    const subscription = preparedSubscription.subscribe();

    const preparedMutation = db.prepareGraphql(
      `
        mutation AddUser($id: Long!, $name: String!) {
          insertUsers(input: [{ id: $id, name: $name }]) {
            id
            name
          }
        }
      `,
      'AddUser'
    );

    const inserted = preparedMutation.exec({ id: 2, name: 'Bob' });
    expect(dataOf(inserted)).toEqual({
      insertUsers: [{ id: 2, name: 'Bob' }],
    });

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 2, name: 'Bob' }],
    });
  });

  it('supports include and skip directives with variables', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]).exec();
    await db.insert('posts').values([
      { id: 10, author_id: 1, title: 'Hello' },
      { id: 11, author_id: 2, title: 'World' },
    ]).exec();

    const query = `
      query Feed($showUsers: Boolean!, $showPosts: Boolean!, $hideName: Boolean!) {
        users @include(if: $showUsers) {
          id
          name @skip(if: $hideName)
          posts @include(if: $showPosts) {
            id
            title
          }
        }
        posts @include(if: $showPosts) {
          id
        }
      }
    `;

    const withUsersOnly = db.graphql(query, {
      showUsers: true,
      showPosts: false,
      hideName: true,
    });
    expect(dataOf(withUsersOnly)).toEqual({
      users: [
        { id: 1 },
        { id: 2 },
      ],
    });

    const withoutRoots = db.graphql(query, {
      showUsers: false,
      showPosts: false,
      hideName: false,
    });
    expect(dataOf(withoutRoots)).toEqual({});
  });

  it('applies directive pruning before validating subscription root selection', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
    ]).exec();

    const subscription = db.subscribeGraphql(
      `
        subscription Feed($showUsers: Boolean!, $showPosts: Boolean!) {
          users @include(if: $showUsers) {
            id
            name
          }
          posts @include(if: $showPosts) {
            id
          }
        }
      `,
      {
        showUsers: true,
        showPosts: false,
      }
    );

    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice' }],
    });
  });
});
