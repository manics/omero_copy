#!/usr/bin/env python

import logging
import re

from omero.rtypes import wrap, unwrap, rstring

logging.basicConfig()
log = logging.getLogger('OmeroCopy')


def update_obj_map(conn, o, obj_map=None):
    if obj_map is None:
        obj_map = {}

    if isinstance(o, str):
        objs = conn.getObjects(o)
    else:
        objs = o

    for obj in objs:
        #desc = obj.getDescription().split('\n')
        #for d in desc:
        desc = obj.getDescription()
        pattern = '\[remote-source:(%s):(\d+)\]' % get_type_name(obj)
        m = re.search(pattern, desc)
        if m:
            name = m.group(1)
            id = long(m.group(2))
            obj_map[(name, id)] = obj
            log.debug('Added [%d] as (%s, %d)', obj.getId(), name, id)
        else:
            log.debug('Ignoring [%d]', obj.getId())

    return obj_map

def get_type_name(obj):
    try:
        obj = obj._obj
    except AttributeError:
        pass
    return obj.__class__.__name__

def get_type_id(obj):
    return get_type_name(obj), unwrap(obj.getId())

# TODO: Use an annotation annotation instead
def add_source_to_description(obj, description):
    s = '[remote-source:%s:%d]' % get_type_id(obj)
    return description + '\n\n' + s

