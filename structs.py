from protos import structs_pb2

_PRIMITIVES = {str, float, int, bytes, bool}


def is_primitive(type_):
    if type_ in _PRIMITIVES:
        return True
    return False


def get_primitive(type_):
    return type_.__name__


def get_fields(obj):
    # todo: handle lists as well!

    if isinstance(obj, dict):
        return obj.items()
    else:
        return obj.__dict__.items()


def to_struct(obj):
    type_ = type(obj)

    if is_primitive(type_):

        return structs_pb2.Primitive(
            type=get_primitive(type_),
            value=str(obj))
    else:
        return structs_pb2.Struct(
            name=type_.__name__,
            fields=[structs_pb2.Field(
                is_primitive=is_primitive(type(value)),
                type=type(value).__name__,
                name=name,
                value=to_struct(value).SerializeToString())
                for name, value in get_fields(obj)]
        )


def create_data(obj):
    return structs_pb2.Data(
        is_primitive=is_primitive(type(obj)),
        value=to_struct(obj).SerializeToString())

