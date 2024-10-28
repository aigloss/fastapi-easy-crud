from typing import Generic, TypeVar, Generator

from sqlalchemy import Engine, inspect
from sqlalchemy.orm import Session
from sqlmodel import SQLModel


def transactional(origin_func):
    def wrapper_func(*args, **kwargs):
        session = args[-1] if type(args[-1]) == Session else kwargs['session'] if 'session' in kwargs else None
        if session is None:
            with Session(args[0].engine) as session:
                res = origin_func(*args, session=session, **kwargs)
                session.commit()
                if res:
                    if type(res) == list:
                        for r in res:
                            session.refresh(r)
                    else:
                        session.refresh(res)
                return res
        else:
            return origin_func(*args, **kwargs)

    return wrapper_func


T = TypeVar("T", bound=SQLModel)


class NotFoundError(ValueError):
    pass


class BaseRepository(Generic[T]):
    """
    Base class for repositories that interact with the database.
    """
    engine: Engine

    def __init__(self, _model_type: type[T], engine: Engine):
        self.engine = engine
        self._model_type: type[T] = _model_type

    def _get_keys_dict(self, item: T) -> dict:
        return dict(
            zip([i.name for i in inspect(self._model_type).primary_key],
                inspect(self._model_type).primary_key_from_instance(item)))

    def _get_keys_from_dict(self, props: dict) -> dict:
        return {k: props[k] for k in (pk.name for pk in inspect(self._model_type).primary_key)}

    # def _get_props_dict(self, props: dict) -> dict:
    #     keys_list = [pk.name for pk in inspect(ClientId).primary_key]
    #     return {k: v for k, v in props.items() if k not in keys_list}

    # @property
    # def _model_type(self) -> type[T]:
    #     return get_args(type(self).__orig_bases__[0])[0]

    @transactional
    def add(self, item: T, session: Session = None) -> T:
        session.add(item)
        session.commit()
        session.refresh(item)
        return item

    @transactional
    def delete(self, keys: dict, session: Session = None) -> None:
        item = self.get(keys)
        if item:
            session.delete(item)
        else:
            raise NotFoundError(f"Item not found for {keys}")

    @transactional
    def update(self, item: T, session: Session = None) -> T:
        item_db = self.get(self._get_keys_dict(item), session)
        if item_db:
            item_data = item.model_dump()
            item_db.sqlmodel_update(item_data)

        return item_db

    @transactional
    def patch(self, keys: dict, session: Session = None, **kwargs) -> T | None:
        res = self.find(session, **keys)
        if res is None:
            raise NotFoundError(f"Item not found for {self._get_keys_dict(**kwargs)}")
        if len(res) > 1:
            raise ValueError(f"Multiple items found for {self._get_keys_dict(*kwargs)}")
        item_db = res[0]
        item_db.sqlmodel_update(kwargs)
        return item_db

    @transactional
    def get(self, keys: dict, session: Session = None) -> T | None:
        return session.get(entity=self._model_type, ident=keys)

    @transactional
    def find(self, session: Session = None, **kwargs) -> list[T]:
        filter_by = {k: v for k, v in kwargs.items() if v is not None}
        return session.query(self._model_type).filter_by(**filter_by).all()
