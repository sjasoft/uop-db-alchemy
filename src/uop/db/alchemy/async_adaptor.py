from uop.db.alchemy import adaptor
from uop.core.async_path import db_collection as base

class A_AlchemyCollection(base.DBCollection, adaptor.TableUtils):
    def __init__(self, db, table, indexed=False, *constraints):
        adaptor.TableUtils.__init__(self, table, db)
        base.DBCollection.__init__(self, db, table, indexed, *constraints)

    async def execute_sql(self, stmt, commit=False):
        if self.in_long_transaction():
            return self._db._connection.execute(stmt)
        else:
            async with await self._engine.connect() as c:
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
        rows = [r for r in await self.execute_sql(stmt)]
        return self.process_rows(rows, only_cols, ids_only)

    async def get(self, an_id):
        stmt = self._table.select().where(self._table.c.id == an_id)
        rows = [r for r in await self.execute_sql(stmt)]
        return rows[0] if rows else None

