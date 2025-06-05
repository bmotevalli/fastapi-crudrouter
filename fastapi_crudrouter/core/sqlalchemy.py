from typing import Any, Callable, List, Type, Generator, Optional, Union
import inspect

from fastapi import Depends, HTTPException, Body
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
    def __init__(
        self,
        schema: Type[SCHEMA],
        db_model: Model,
        db: "Session",
        create_schema: Optional[Type[SCHEMA]] = None,
        update_schema: Optional[Type[SCHEMA]] = None,
        prefix: Optional[str] = None,
        tags: Optional[List[str]] = None,
        paginate: Optional[int] = None,
        get_all_route: Union[bool, DEPENDENCIES] = True,
        get_one_route: Union[bool, DEPENDENCIES] = True,
        create_route: Union[bool, DEPENDENCIES] = True,
        update_route: Union[bool, DEPENDENCIES] = True,
        delete_one_route: Union[bool, DEPENDENCIES] = True,
        delete_all_route: Union[bool, DEPENDENCIES] = True,
        **kwargs: Any
    ) -> None:
        assert sqlalchemy_installed, "SQLAlchemy must be installed to use the SQLAlchemyCRUDRouter."

        self.db_model = db_model
        self.db_func = db
        self._pk: str = db_model.__table__.primary_key.columns.keys()[0]
        self._pk_type: type = _utils.get_pk_type(schema, self._pk)

        # Pass only the expected arguments to CRUDGenerator
        super().__init__(
            schema=schema,
            create_schema=create_schema,
            update_schema=update_schema,
            prefix=prefix or db_model.__tablename__,
            tags=tags,
            paginate=paginate,
            get_all_route=get_all_route,
            get_one_route=get_one_route,
            create_route=create_route,
            update_route=update_route,
            delete_one_route=delete_one_route,
            delete_all_route=delete_all_route,
            **kwargs
        )

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
            model: self.create_schema = Body(...), db: Union[Session, AsyncSession] = Depends(self.db_func)
        ) -> Model:
            try:
                db_model: Model = self.db_model(**model.model_dump())
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
            model: self.update_schema = Body(...),
            db: Union[Session, AsyncSession] = Depends(self.db_func),
        ) -> Model:
            db_model: Model = await self._get_one()(item_id, db)

            for key, value in model.model_dump(exclude={self._pk}).items():
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
