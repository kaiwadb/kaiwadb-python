import datetime as dt
import uuid
from enum import Enum
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin, overload

from pydantic import BaseModel

from kaiwadb.document import Document
from kaiwadb.models.instance import (
    ArrayField,
    EnumField,
    ObjectField,
    PrimitiveField,
    PrimitiveType,
    Table,
    UnionField,
    Variant,
)
from kaiwadb.types.object_id import ObjectId


def map_documents_to_tables(documents: list[type[Document]]) -> list[Table]:
    return [
        Table(
            name=doc.__collection__ or doc.__table__ or doc.__name__,
            alias=doc.__name__,
            fields=map_to_type(doc).properties,
        )
        for doc in documents
    ]


@overload
def map_to_type(
    annotation: type[Document],
    alias: str | None = None,
    description: str | None = None,
    optional: bool = False,
) -> ObjectField: ...


@overload
def map_to_type(  # pyright: ignore[reportOverlappingOverload]
    annotation: type, alias: str | None = None, description: str | None = None, optional: bool = False
) -> ObjectField | ArrayField | UnionField | EnumField | PrimitiveField: ...


def map_to_type(
    annotation: type[Any], alias: str | None = None, description: str | None = None, optional: bool = False
) -> ObjectField | ArrayField | UnionField | EnumField | PrimitiveField:
    PRIMITIVES = {
        bool: PrimitiveType.BOOL,
        int: PrimitiveType.INTEGER,
        float: PrimitiveType.FLOAT,
        str: PrimitiveType.STRING,
        dt.datetime: PrimitiveType.DATETIME,
        dt.date: PrimitiveType.DATE,
        dt.time: PrimitiveType.TIME,
        ObjectId: PrimitiveType.OID,
        uuid.UUID: PrimitiveType.UUID,
    }

    if (origin := get_origin(annotation)) is not None:
        if origin in [Union, UnionType]:
            args = set(get_args(annotation))
            if optional := NoneType in args:
                args -= {NoneType}

            if len(args) == 1:
                return map_to_type(args.pop(), alias=alias, description=description, optional=optional)

            return UnionField(
                alias=alias,
                description=description,
                optional=optional,
                types=[map_to_type(a) for a in args],
            )

        if origin is list:
            args = get_args(annotation)
            if len(args) == 1:
                return ArrayField(
                    alias=alias, description=description, optional=optional, item=map_to_type(args[0])
                )

    if annotation in PRIMITIVES:
        return PrimitiveField(
            alias=alias, description=description, optional=optional, type=PRIMITIVES[annotation]
        )

    if issubclass(annotation, Enum):
        return EnumField(
            alias=alias,
            optional=optional,
            description=description,
            name=annotation.__name__,
            variants=[
                Variant(value=variant.value, alias=variant.name if variant.name != variant.value else None)
                for variant in annotation
            ],
        )

    if issubclass(annotation, BaseModel):
        return ObjectField(
            optional=optional,
            alias=alias,
            description=description,
            properties={
                field.alias or name: map_to_type(field.annotation, name, field.description)
                for name, field in annotation.model_fields.items()
                if field.annotation is not None
            },
        )

    raise NotImplementedError(f"KaiwaDB does not support {annotation}")
