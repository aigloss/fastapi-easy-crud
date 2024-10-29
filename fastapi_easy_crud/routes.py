import inspect as insp
from functools import lru_cache
from typing import TypeVar

from fastapi import FastAPI
from makefun import create_function
from pydantic import create_model
from sqlalchemy import inspect, Column
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel
from starlette.exceptions import HTTPException

from .repository import NotFoundError, BaseRepository

T = TypeVar("T", bound=SQLModel)


def get_type(c: Column):
    """
    Returns de python type associated with a Column object.
    If the type is not a builtin type, that type is imported to make it available in scope.
    """
    t = c.type
    if hasattr(t, 'impl'):
        t = t.impl
    t = t.python_type
    import_type(t)

    return t.__qualname__


def import_type(t: type):
    module = t.__module__
    if module != 'builtins':
        # import required types to make it available to create_function
        exec(f'from {module} import {t.__qualname__}', globals())


@lru_cache
def get_key_pairs(model_type: type[T]):
    """
    Returns a list of tuples with the name and type of the primary keys of a type.
    """
    return [(pk.name, get_type(pk)) for pk in inspect(model_type).primary_key]


@lru_cache
def get_request_model(model_type: type[T], all_nullable: bool = False):
    """
    Creates a Pydantic class with the same attributes as the _model_type_.

    :param model_type: The type to create the request model from.
    :param all_nullable: If True, all attributes are optional.

    SQLModel instances are not validated by default by FastAPI, this function
    helps creating a "request" version of the model to be used in FastAPI body
    parameters.
    """

    model_props = {
        c.name: (get_type(c), None) if all_nullable or c.nullable else (get_type(c), ...)
        for c in (k.columns[0] for k in inspect(model_type).attrs)
    }
    model = create_model(model_type.__name__ + "RequestData", **model_props)

    # register the new type under the current module to make it available
    current_module = insp.getmodule(insp.stack()[1][0])
    setattr(current_module, model.__name__, model)

    return model


def add_patch_route(app: FastAPI, base_path: str, model_type: type[T], repository: BaseRepository[T]):
    # create a Pydantic class with the same attributes as the model_type, but removing
    # those that are primary keys and with all of them as optional
    model_keys = [(pk.name, get_type(pk)) for pk in inspect(model_type).primary_key]
    model_props = {c.name: (get_type(c), None) for c in (k.columns[0] for k in inspect(model_type).attrs if
                                                         k.columns[0].name not in [pk[0] for pk in model_keys])}
    patch_model = create_model(model_type.__name__ + "PatchData", **model_props)

    # register the new type under the current module to make it available
    current_module = insp.getmodule(insp.stack()[1][0])
    setattr(current_module, patch_model.__name__, patch_model)

    # create the function signature for the endpoint
    model_keys_str = ','.join([f'{k}:{v}' for k, v in model_keys])
    func_sig = f'update_item_attributes({model_keys_str}, item: {patch_model.__name__})'

    def default_patch(*args, **kwargs):
        return repository.patch(keys={k: v for k, v in kwargs.items() if k != 'item'},
                                **kwargs.get('item').dict(exclude_none=True))

    doc = f"Update one or more attributes of a given {model_type.__name__} item"
    app.add_api_route(f'{base_path}', create_function(func_sig, default_patch, doc=doc), methods=["PATCH"],
                      response_model=model_type, tags=[model_type.__name__])


def add_delete_route(app: FastAPI, base_path: str, model_type: type[T], repository: BaseRepository[T]):
    func_sig = f'delete({",".join(f"{k}: {v}" for k, v in get_key_pairs(model_type))})'

    def default_delete(*args, **kwargs):
        try:
            return repository.delete(keys=kwargs)
        except NotFoundError:
            raise HTTPException(status_code=404, detail="Item not found")

    doc = f"Delete a {model_type.__name__} item by its primary key"
    app.add_api_route(f'{base_path}', create_function(func_sig, default_delete,doc=doc), methods=["DELETE"],
                      response_model=None, tags=[model_type.__name__])


def add_put_route(app: FastAPI, base_path: str, model_type: type[T], repository: BaseRepository[T]):
    request_model = get_request_model(model_type)
    func_sig = f'update_item({model_type.__name__.lower()}: {request_model.__name__})'

    def default_put(*args, **kwargs):
        return repository.update(model_type(**kwargs.get(model_type.__name__.lower()).dict()))

    doc = f"Update all the attributes of a given {model_type.__name__} item"
    app.add_api_route(f'{base_path}', create_function(func_sig, default_put, doc=doc), methods=["PUT"],
                      response_model=model_type, tags=[model_type.__name__])


def add_post_route(app: FastAPI, base_path: str, model_type: type[T], repository: BaseRepository[T]):
    request_model = get_request_model(model_type)
    func_sig = f'create({model_type.__name__.lower()}: {request_model.__name__})'

    def default_post(*args, **kwargs):
        try:
            return repository.add(model_type(**kwargs.get(model_type.__name__.lower()).dict()))
        except IntegrityError as e:
            raise HTTPException(status_code=409, detail=e.orig.args[0])

    doc = f"Create a new {model_type.__name__} item"
    app.add_api_route(f'{base_path}', create_function(func_sig, default_post, doc=doc), methods=["POST"],
                      response_model=model_type, tags=[model_type.__name__])


def add_get_route(app: FastAPI, base_path: str, model_type: type[T], repository: BaseRepository[T]):
    keys = '/'.join([f'{{{k}:{v}}}' for k, v in get_key_pairs(model_type)])
    func_sig = f'get({",".join(f"{k}: {v}" for k, v in get_key_pairs(model_type))})'

    def default_get(*args, **kwargs):
        res = repository.get(keys=kwargs)
        if res is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return res

    import_type(model_type)
    doc = f"Get a {model_type.__name__} item by its primary key"
    app.add_api_route(f'{base_path}/{keys}', create_function(func_sig, default_get, doc=doc), methods=["GET"],
                      response_model=model_type, tags=[model_type.__name__])


def add_find_route(app: FastAPI, base_path: str, model_type: type[T], repository: BaseRepository[T]):
    model_props = [(c.name, get_type(c)) for c in (k.columns[0] for k in inspect(model_type).attrs)]
    params = ','.join([f'{k}: {v} = None' for k, v in model_props])
    func_sig = f'find({params})'

    def default_find(*args, **kwargs):
        return repository.find(**kwargs)

    import_type(model_type)
    doc = f"Find all {model_type.__name__} items that match any of the filter values"
    app.add_api_route(f'{base_path}', create_function(func_sig, default_find, doc=doc), methods=["GET"],
                      response_model=list[model_type] | None, tags=[model_type.__name__])


def add_base_crud_endpoints(app: FastAPI, model_type: type[T], base_path: str, repository: BaseRepository[T]):
    # FIND
    add_find_route(app=app, base_path=base_path, model_type=model_type, repository=repository)

    # GET
    add_get_route(app=app, base_path=base_path, model_type=model_type, repository=repository)

    # POST
    add_post_route(app=app, base_path=base_path, model_type=model_type, repository=repository)

    # PUT
    add_put_route(app=app, base_path=base_path, model_type=model_type, repository=repository)

    # DELETE
    add_delete_route(app=app, base_path=base_path, model_type=model_type, repository=repository)

    # PATCH
    add_patch_route(app=app, base_path=base_path, model_type=model_type, repository=repository)

