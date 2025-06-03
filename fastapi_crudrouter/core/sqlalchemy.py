from typing import Any, Callable, List, Type, Generator, Optional, Union
import inspect

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import CRUDGenerator, NOT_FOUND, _utils
from ._types import DEPENDENCIES, PAGINATION, PYDANTIC_SCHEMA as SCHEMA

try:
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import DeclarativeMeta as Model
    from sqlalchemy.exc import IntegrityError
except ImportError:
    Model = None
    Session = None
    IntegrityError = None
    sqlalchemy_installed = False
else:
    sqlalchemy_installed = True
    Session = Callable[..., Generator[Session, Any, None]]

CALLABLE = Callable[..., Model]
CALLABLE_LIST = Callable[..., List[Model]]


class SQLAlchemyCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        assert sqlalchemy_installed, "SQLAlchemy must be installed to use the SQLAlchemyCRUDRouter."
        super().__init__(*args, **kwargs)
        self._pk: str = self.db_model.__table__.primary_key.columns.keys()[0]
        self._pk_type: type = _utils.get_pk_type(self.schema, self._pk)

    def _get_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route(
            db: Union[Session, AsyncSession] = Depends(self.db_func),
            pagination: PAGINATION = self.pagination,
        ) -> List[Model]:
            skip, limit = pagination.get("skip"), pagination.get("limit")

            if isinstance(db, AsyncSession):
                result = await db.execute(
                    select(self.db_model).order_by(getattr(self.db_model, self._pk)).offset(skip).limit(limit)
                )
                return result.scalars().all()
            else:
                return (
                    db.query(self.db_model)
                    .order_by(getattr(self.db_model, self._pk))
                    .offset(skip)
                    .limit(limit)
                    .all()
                )

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            item_id: self._pk_type, db: Union[Session, AsyncSession] = Depends(self.db_func)
        ) -> Model:
            if isinstance(db, AsyncSession):
                result = await db.execute(select(self.db_model).where(getattr(self.db_model, self._pk) == item_id))
                model = result.scalar_one_or_none()
            else:
                model = db.query(self.db_model).get(item_id)

            if model:
                return model
            else:
                raise NOT_FOUND from None

        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            model: self.create_schema, db: Union[Session, AsyncSession] = Depends(self.db_func)
        ) -> Model:
            try:
                db_model: Model = self.db_model(**model.dict())
                db.add(db_model)
                if isinstance(db, AsyncSession):
                    await db.commit()
                    await db.refresh(db_model)
                else:
                    db.commit()
                    db.refresh(db_model)
                return db_model
            except IntegrityError:
                if isinstance(db, AsyncSession):
                    await db.rollback()
                else:
                    db.rollback()
                raise HTTPException(422, "Key already exists") from None

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            item_id: self._pk_type,
            model: self.update_schema,
            db: Union[Session, AsyncSession] = Depends(self.db_func),
        ) -> Model:
            db_model: Model = await self._get_one()(item_id, db)

            for key, value in model.dict(exclude={self._pk}).items():
                if hasattr(db_model, key):
                    setattr(db_model, key, value)

            try:
                if isinstance(db, AsyncSession):
                    await db.commit()
                    await db.refresh(db_model)
                else:
                    db.commit()
                    db.refresh(db_model)
                return db_model
            except IntegrityError as e:
                if isinstance(db, AsyncSession):
                    await db.rollback()
                else:
                    db.rollback()
                self._raise(e)

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route(db: Union[Session, AsyncSession] = Depends(self.db_func)) -> List[Model]:
            if isinstance(db, AsyncSession):
                await db.execute(self.db_model.__table__.delete())
                await db.commit()
                return await self._get_all()(db=db, pagination={"skip": 0, "limit": None})
            else:
                db.query(self.db_model).delete()
                db.commit()
                return self._get_all()(db=db, pagination={"skip": 0, "limit": None})

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(
            item_id: self._pk_type, db: Union[Session, AsyncSession] = Depends(self.db_func)
        ) -> Model:
            db_model: Model = await self._get_one()(item_id, db)

            if isinstance(db, AsyncSession):
                await db.delete(db_model)
                await db.commit()
            else:
                db.delete(db_model)
                db.commit()

            return db_model

        return route
