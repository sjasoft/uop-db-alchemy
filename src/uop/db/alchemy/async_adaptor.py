from uop.db.alchemy import adaptor
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from uop.core.async_path import db_collection as base
from uop.core.async_path import database
import json


class A_AlchemyCollection(base.DBCollection, adaptor.TableUtils):
    def __init__(self, db, table, indexed=False, *constraints):
        adaptor.TableUtils.__init__(self, table, db)
        base.DBCollection.__init__(self, db, table, indexed, *constraints)

    async def execute_sql(self, stmt, commit=False):
        if self.in_long_transaction():
            return await self._db._connection.execute(stmt)
        else:
            async with self._engine.connect() as c:
                try:
                    res = await c.execute(stmt)
                    if commit:
                        await c.commit()
                    return res
                except Exception as e:
                    print(f"Error executing sql: {e}")
                    await c.rollback()
                    raise e

    async def insert(self, **fields):
        return await self.execute_sql(self.insert_stmt(**fields), commit=True)

    async def remove(self, dict_or_key):
        stmt = self.delete_stmt(dict_or_key)
        return await self.execute_sql(stmt, commit=True)

    async def remove_all(self):
        await self.execute_sql(self._table.delete())

    async def update(self, selector, mods, partial=True):
        stmt = self.update_stmt(selector, mods)
        return await self.execute_sql(stmt, commit=True)

    async def update_one(self, key, mods):
        return await self.update({"id": key}, mods)

    async def find(
        self, criteria=None, only_cols=None, order_by=None, limit=None, ids_only=False
    ):
        stmt = self.select_stmt(criteria, only_cols, order_by, limit)
        result = await self.execute_sql(stmt)
        rows = [r for r in result]
        return self.process_rows(rows, only_cols, ids_only)

    async def get(self, an_id):
        stmt = self._table.select().where(self._table.c.id == an_id)
        result = await self.execute_sql(stmt)
        rows = [r for r in result]
        return dict(rows[0]._mapping) if rows else None


class A_AlchemyDatabase(database.Database, adaptor.AlchemyDatabase):
    async def open_db(self):
        self._connection_string = self.get_connection_string()
        self._engine = create_async_engine(
            self._connection_string,
            json_serializer=json.dumps,
            json_deserializer=json.loads,
        )
        self._tables = await self.get_tables()
        self._root_txn = None
        await super().open_db()

    async def get_tables(self):
        metadata = adaptor.Base.metadata
        async with self._engine.connect() as c:
            await c.run_sync(metadata.reflect)
        return metadata.tables

    async def start_long_transaction(self):
        self._connection = await self._engine.connect().__aenter__()
        self._root_txn = await self._connection.begin().__aenter__()

    async def end_long_transaction(self):
        if self._root_txn:
            await self._root_txn.__aexit__(None, None, None)
            await self._connection.__aexit__(None, None, None)
        self._connection = None
        self._root_txn = None
        await super().end_long_transaction()

    async def really_commit(self):
        await self._root_txn.commit()
        # self._root_txn = None

    def get_existing_table(self, table_name):
        return self._tables.get(table_name)

    async def abort(self):
        if self._root_txn:
            await self._root_txn.rollback()
        await self.end_long_transaction()

    def wrap_raw_collection(self, raw):
        return A_AlchemyCollection(self, raw)

    async def get_raw_collection(self, name, schema):
        existing = self.get_existing_table(name)
        if existing is None:
            existing = adaptor.table_from_schema(schema, name)
            async with self._engine.begin() as c:
                await c.run_sync(existing.create)
        return existing
