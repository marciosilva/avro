# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Specific record support.
"""
import types
from avro import schema
from avro.io import validate, AvroTypeException

TEST_SCHEMA = '''
{"name":"fooRecord",
 "type":"record",
 "fields": [
        {"name":"f1","type":"int"},
        {"name":"f2","type":"long"}
 ]
}
'''

def specific_record(rec_schema):
  """
  Constructs a specific record class for a given avro schema
  
  Args:
    rec_schema an avro schema object from which to construct the record

  Returns: 
    A class representing the avro record
  """
  return SpecificRecordMeta(rec_schema.fullname.encode('utf-8'),
                            (object,),{'schema':rec_schema})

class SpecificRecordBase(object):
  """
  Base class for specific records. Provides access to the schema,
  and defines common methods for property access.
  """

  __slots__ = []

  def __init__(self):
    self._fields = {}

  @classmethod
  def validate(cls,instance):
    for field in cls.get_schema().fields:
      field_name = str(field.name)
      value = instance.get(field_name)
      if not validate(field.type,value):
        raise AvroTypeException(field.type, value)

  @classmethod
  def get_schema(self):
    """
    Retrieve the avro schema for this record
    """
    return getattr(self,'_schema')

  def __getitem__(self,key):
    return self._fields.__getitem__(key)

  def __setitem__(self, key, value):
    setattr(self,key,value)

  def keys(self):
    return self._fields.keys()

  def __eq__(self, other):
    if isinstance(other,self.__class__):
      return self._fields.eq(other._fields)
    else:
      return false

  def __ne__(self,other):
    return not self.__eq__


class SpecificRecordMeta(type):
  """
  Metaclass for generating specific records.
  Can be used dynamically or be creating a concrete class.

  Example::

      class ExampleRecord(object):
          __metaclass__ = SpecificRecordMeta
          schema = schema.parse('''
                    {"name": "ExampleRecord",
                     "type": "record",
                     "doc" : "My Specific Record type"
                     "fields": [ {"name": "field1", "type": "int" },
                                 {"name": "field2", "type": "float"}
                               ]}
                                ''')
  """

  def __wrap_property(meta, field):
    """
    Wraps a property accessor in schema validation logic
    """
    if hasattr(field,'doc'):
        doc = field.doc
    else:
        doc = 'Generated property for the %s field' % field.name

    field_type = field.type

    def getter(self):
      # TODO: (msilva) Decide if we should add default value
      #       handling, or should we leave it in the DatumWriter
      return self._fields.get(field.name)

    def setter(self,value):
      if validate(field_type,value):
        self._fields[field.name] = value
      else:
        raise AvroTypeException(field_type, value)

    getter.__doc__ = 'Getter for the %s field' % (field.name)
    setter.__doc__ = 'Setter for the %s field' % (field.name)
    return property(getter, setter, None, doc)

  def __new__(cls,name,bases,d):
    """
    Called before class is generated
    """
    schema = d['schema']
    d.clear()
    d['_schema'] = schema
    d['__slots__'] = [ '_fields', '__weakref__' ]
    return super(SpecificRecordMeta,cls).__new__(cls,name,(SpecificRecordBase,)+ bases,d)

  def __init__(cls,name,bases,d):
    """
    """
    super(SpecificRecordMeta,cls).__init__(name,bases,d)
    for field in d['_schema'].fields:
      attr = field.name.encode('utf-8')
      wrapped = cls.__wrap_property(field)
      setattr(cls,attr,wrapped)
    cls.__module__ = None

if __name__ == '__main__':
    s = schema.parse(TEST_SCHEMA)
    c = specific_record(s)
    instance = c()
    help(instance)
    print c._schema
    instance.f1 = 1
    instance.f1 = 'bad'
