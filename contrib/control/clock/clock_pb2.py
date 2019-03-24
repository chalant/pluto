# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: contrib/control/clock/clock.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from contrib.trading_calendars.protos import calendar_pb2 as contrib_dot_trading__calendars_dot_protos_dot_calendar__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='contrib/control/clock/clock.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n!contrib/control/clock/clock.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1bgoogle/protobuf/empty.proto\x1a/contrib/trading_calendars/protos/calendar.proto\"\x1d\n\x04Rate\x12\x15\n\remission_rate\x18\x01 \x01(\t\"R\n\nClockEvent\x12-\n\ttimestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x15\n\x05\x65vent\x18\x02 \x01(\x0e\x32\x06.Event\"#\n\nAttributes\x12\x15\n\rcalendar_name\x18\x01 \x01(\t\"6\n\x10RegistrationForm\x12\x0b\n\x03url\x18\x01 \x01(\t\x12\x15\n\rcalendar_name\x18\x02 \x01(\t*\x95\x01\n\x05\x45vent\x12\x0e\n\nINITIALIZE\x10\x00\x12\x11\n\rSESSION_START\x10\x01\x12\x07\n\x03\x42\x41R\x10\x02\x12\x0e\n\nMINUTE_END\x10\x03\x12\x0f\n\x0bSESSION_END\x10\x04\x12\x18\n\x14\x42\x45\x46ORE_TRADING_START\x10\x05\x12\r\n\tLIQUIDATE\x10\x06\x12\x08\n\x04STOP\x10\x07\x12\x0c\n\x08\x43\x41LENDAR\x10\x08\x32\xd6\x01\n\x0b\x43lockServer\x12/\n\x06Listen\x12\x16.google.protobuf.Empty\x1a\x0b.ClockEvent0\x01\x12\x30\n\x0bGetCalendar\x12\x16.google.protobuf.Empty\x1a\t.Calendar\x12-\n\x0c\x45missionRate\x12\x16.google.protobuf.Empty\x1a\x05.Rate\x12\x35\n\x08Register\x12\x11.RegistrationForm\x1a\x16.google.protobuf.Empty2<\n\x0b\x43lockClient\x12-\n\x06Update\x12\x0b.ClockEvent\x1a\x16.google.protobuf.Emptyb\x06proto3')
  ,
  dependencies=[google_dot_protobuf_dot_timestamp__pb2.DESCRIPTOR,google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,contrib_dot_trading__calendars_dot_protos_dot_calendar__pb2.DESCRIPTOR,])

_EVENT = _descriptor.EnumDescriptor(
  name='Event',
  full_name='Event',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='INITIALIZE', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SESSION_START', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BAR', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='MINUTE_END', index=3, number=3,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SESSION_END', index=4, number=4,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BEFORE_TRADING_START', index=5, number=5,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='LIQUIDATE', index=6, number=6,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='STOP', index=7, number=7,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='CALENDAR', index=8, number=8,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=357,
  serialized_end=506,
)
_sym_db.RegisterEnumDescriptor(_EVENT)

Event = enum_type_wrapper.EnumTypeWrapper(_EVENT)
INITIALIZE = 0
SESSION_START = 1
BAR = 2
MINUTE_END = 3
SESSION_END = 4
BEFORE_TRADING_START = 5
LIQUIDATE = 6
STOP = 7
CALENDAR = 8



_RATE = _descriptor.Descriptor(
  name='Rate',
  full_name='Rate',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='emission_rate', full_name='Rate.emission_rate', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
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
  serialized_start=148,
  serialized_end=177,
)


_CLOCKEVENT = _descriptor.Descriptor(
  name='ClockEvent',
  full_name='ClockEvent',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='ClockEvent.timestamp', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='event', full_name='ClockEvent.event', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
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
  serialized_start=179,
  serialized_end=261,
)


_ATTRIBUTES = _descriptor.Descriptor(
  name='Attributes',
  full_name='Attributes',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='calendar_name', full_name='Attributes.calendar_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
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
  serialized_start=263,
  serialized_end=298,
)


_REGISTRATIONFORM = _descriptor.Descriptor(
  name='RegistrationForm',
  full_name='RegistrationForm',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='url', full_name='RegistrationForm.url', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='calendar_name', full_name='RegistrationForm.calendar_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
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
  serialized_start=300,
  serialized_end=354,
)

_CLOCKEVENT.fields_by_name['timestamp'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_CLOCKEVENT.fields_by_name['event'].enum_type = _EVENT
DESCRIPTOR.message_types_by_name['Rate'] = _RATE
DESCRIPTOR.message_types_by_name['ClockEvent'] = _CLOCKEVENT
DESCRIPTOR.message_types_by_name['Attributes'] = _ATTRIBUTES
DESCRIPTOR.message_types_by_name['RegistrationForm'] = _REGISTRATIONFORM
DESCRIPTOR.enum_types_by_name['Event'] = _EVENT
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Rate = _reflection.GeneratedProtocolMessageType('Rate', (_message.Message,), dict(
  DESCRIPTOR = _RATE,
  __module__ = 'contrib.control.clock.clock_pb2'
  # @@protoc_insertion_point(class_scope:Rate)
  ))
_sym_db.RegisterMessage(Rate)

ClockEvent = _reflection.GeneratedProtocolMessageType('ClockEvent', (_message.Message,), dict(
  DESCRIPTOR = _CLOCKEVENT,
  __module__ = 'contrib.control.clock.clock_pb2'
  # @@protoc_insertion_point(class_scope:ClockEvent)
  ))
_sym_db.RegisterMessage(ClockEvent)

Attributes = _reflection.GeneratedProtocolMessageType('Attributes', (_message.Message,), dict(
  DESCRIPTOR = _ATTRIBUTES,
  __module__ = 'contrib.control.clock.clock_pb2'
  # @@protoc_insertion_point(class_scope:Attributes)
  ))
_sym_db.RegisterMessage(Attributes)

RegistrationForm = _reflection.GeneratedProtocolMessageType('RegistrationForm', (_message.Message,), dict(
  DESCRIPTOR = _REGISTRATIONFORM,
  __module__ = 'contrib.control.clock.clock_pb2'
  # @@protoc_insertion_point(class_scope:RegistrationForm)
  ))
_sym_db.RegisterMessage(RegistrationForm)



_CLOCKSERVER = _descriptor.ServiceDescriptor(
  name='ClockServer',
  full_name='ClockServer',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=509,
  serialized_end=723,
  methods=[
  _descriptor.MethodDescriptor(
    name='Listen',
    full_name='ClockServer.Listen',
    index=0,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_CLOCKEVENT,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='GetCalendar',
    full_name='ClockServer.GetCalendar',
    index=1,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=contrib_dot_trading__calendars_dot_protos_dot_calendar__pb2._CALENDAR,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='EmissionRate',
    full_name='ClockServer.EmissionRate',
    index=2,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_RATE,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='Register',
    full_name='ClockServer.Register',
    index=3,
    containing_service=None,
    input_type=_REGISTRATIONFORM,
    output_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_CLOCKSERVER)

DESCRIPTOR.services_by_name['ClockServer'] = _CLOCKSERVER


_CLOCKCLIENT = _descriptor.ServiceDescriptor(
  name='ClockClient',
  full_name='ClockClient',
  file=DESCRIPTOR,
  index=1,
  serialized_options=None,
  serialized_start=725,
  serialized_end=785,
  methods=[
  _descriptor.MethodDescriptor(
    name='Update',
    full_name='ClockClient.Update',
    index=0,
    containing_service=None,
    input_type=_CLOCKEVENT,
    output_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_CLOCKCLIENT)

DESCRIPTOR.services_by_name['ClockClient'] = _CLOCKCLIENT

# @@protoc_insertion_point(module_scope)