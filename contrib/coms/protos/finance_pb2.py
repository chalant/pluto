# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: contrib/coms/protos/finance.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from contrib.coms.protos import assets_pb2 as contrib_dot_coms_dot_protos_dot_assets__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='contrib/coms/protos/finance.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n!contrib/coms/protos/finance.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a contrib/coms/protos/assets.proto\"\x91\x01\n\x0bTransaction\x12\x15\n\x05\x61sset\x18\x01 \x01(\x0b\x32\x06.Asset\x12\x0e\n\x06\x61mount\x18\x02 \x01(\x02\x12&\n\x02\x64t\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\r\n\x05price\x18\x04 \x01(\x02\x12\x10\n\x08order_id\x18\x05 \x01(\t\x12\x12\n\ncommission\x18\x06 \x01(\x02\x62\x06proto3')
  ,
  dependencies=[google_dot_protobuf_dot_timestamp__pb2.DESCRIPTOR,contrib_dot_coms_dot_protos_dot_assets__pb2.DESCRIPTOR,])




_TRANSACTION = _descriptor.Descriptor(
  name='Transaction',
  full_name='Transaction',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='asset', full_name='Transaction.asset', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='amount', full_name='Transaction.amount', index=1,
      number=2, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='dt', full_name='Transaction.dt', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='price', full_name='Transaction.price', index=3,
      number=4, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='order_id', full_name='Transaction.order_id', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commission', full_name='Transaction.commission', index=5,
      number=6, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
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
  serialized_start=105,
  serialized_end=250,
)

_TRANSACTION.fields_by_name['asset'].message_type = contrib_dot_coms_dot_protos_dot_assets__pb2._ASSET
_TRANSACTION.fields_by_name['dt'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
DESCRIPTOR.message_types_by_name['Transaction'] = _TRANSACTION
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Transaction = _reflection.GeneratedProtocolMessageType('Transaction', (_message.Message,), dict(
  DESCRIPTOR = _TRANSACTION,
  __module__ = 'contrib.coms.protos.finance_pb2'
  # @@protoc_insertion_point(class_scope:Transaction)
  ))
_sym_db.RegisterMessage(Transaction)


# @@protoc_insertion_point(module_scope)