import dill
import os
import json
from . import core as bundle
from pandas import Timestamp
from bson import json_util

PATH = os.path.join(os.environ['HOME'],'register_functions')
if not os.path.exists(PATH):
	os.mkdir(PATH)

def add_register_func(name,func,calendar_name,start_session, end_session):
	path = os.path.join(PATH,name)
	if not os.path.exists(path):
		os.mkdir(path)
	with open(os.path.join(path,'{}.json'.format(name)),'w+') as file:
		d = {}
		d['name'] = name
		d['calendar_name'] = calendar_name
		d['start_session'] = start_session.to_datetime()
		d['end_session'] = end_session.to_datetime()
		json.dump(d,file,default=json_util.default)
	with open(os.path.join(path,'{}.pkl'.format(name)),'wb+') as file:
		dill.dump(func,file)

def register_all():
	for name in os.listdir(PATH):
		path = os.path.join(PATH,name)
		try:
			d = None
			func = None
			for name1 in os.listdir(path):
				path1 = os.path.join(path,name1)
				if name1.endswith('.json'):
					with open(path1,'r') as f:
						d = json.load(f,object_hook=json_util.object_hook)
				elif name1.endswith('.pkl'):
					with open(path1,'rb') as f:
						func = dill.load(f)
			if d and func:
				bundle.register(name=d['name'], f=func, calendar_name=d['calendar_name'],
								start_session=Timestamp(d['start_session']),
								end_session=Timestamp(d['end_session']))
		except (FileNotFoundError, Exception) as e:
			pass

register_all()