# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: schemas/exchange.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='schemas/exchange.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x16schemas/exchange.proto\"C\n\x08\x45xchange\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x14\n\x0c\x63ountry_code\x18\x02 \x01(\t\x12\x13\n\x0b\x61sset_types\x18\x03 \x03(\tb\x06proto3')
)




_EXCHANGE = _descriptor.Descriptor(
  name='Exchange',
  full_name='Exchange',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='Exchange.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='country_code', full_name='Exchange.country_code', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='asset_types', full_name='Exchange.asset_types', index=2,
      number=3, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=26,
  serialized_end=93,
)

DESCRIPTOR.message_types_by_name['Exchange'] = _EXCHANGE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Exchange = _reflection.GeneratedProtocolMessageType('Exchange', (_message.Message,), dict(
  DESCRIPTOR = _EXCHANGE,
  __module__ = 'schemas.exchange_pb2'
  # @@protoc_insertion_point(class_scope:Exchange)
  ))
_sym_db.RegisterMessage(Exchange)


# @@protoc_insertion_point(module_scope)
