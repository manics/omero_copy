#!/usr/bin/env python

import argparse
import logging
import os
import re
import sys

import omero
import omero.gateway

from omero.rtypes import unwrap, rstring



"""
Copy tags and tagsets between OMERO servers
Two ice-config files (source and destination) must be passed, each containing
either
  omero.user=username
  omero.pass=password
  omero.host=hostname
or
  omero.user=sessionid
  omero.pass=sessionid
  omero.host=sessionid
where sessionid can be obtained from
  cat `omero sessions file` |grep omero.sess
assuming a session has already been created
"""

logging.basicConfig()
log = logging.getLogger('OmeroCopy')

def parseArgs():
    parser = argparse.ArgumentParser(
        description='Export images as OME.TIFFs from datasets or projects')
    parser.add_argument('-d', help='Debug logging',
                        action='store_true')
    parser.add_argument('-n', help='Dry-run, show what would be done',
                        action='store_true')
    parser.add_argument('ic1', help='ice.config for the source')
    parser.add_argument('ic2', help='ice.config for the destination')
    parser.add_argument('-abort', help='Abort on exception',
                        action='store_true')
    args = parser.parse_args()

    if args.d:
        log.setLevel(logging.DEBUG)
    log.debug('ice.config 1:%s 2:%s%s%s%s',
              args.ic1, args.ic2, ' (dryrun)' if args.n else '',
              ' (debug)' if args.d else '',
              ' (abort exceptions)' if args.abort else '')
    return args


def get_connection(ic1):
    cli = omero.client(args=['--Ice.Config=%s' % ic1])
    sess = cli.createSession()
    conn = omero.gateway.BlitzGateway(client_obj=cli)
    conn.SERVICE_OPTS.setOmeroGroup(-1)
    return conn


def get_tags(conn):
    try:
        tags = list(conn.getObjects('TagAnnotation'))
        for tag in tags:
            log.debug('Found tag id:%d value:%s ns:%s name:%s description:%s',
                      tag.getId(), tag.getValue(), tag.getNs(),
                      tag.getName(), tag.getDescription())

        return tags

    except Exception as e:
        log.error('Failed to get tags: %s', e)

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
        pattern = '\[remote-source:(%s):(\d+)\]' % obj.OMERO_TYPE.__name__
        m = re.search(pattern, desc)
        if m:
            name = m.group(1)
            id = long(m.group(2))
            obj_map[(name, id)] = obj
            log.debug('Added [%d] as (%s, %d)', obj.getId(), name, id)
        else:
            log.debug('Ignoring [%d]', obj.getId())

    return obj_map

def get_type_id(obj):
    return obj.OMERO_TYPE.__name__, obj.getId()

# TODO: Use an annotation annotation instead
def add_source_text_to_description(obj, description):
    s = '[remote-source:%s:%d]' % get_type_id(obj)
    return description + '\n\n' + s

def create_tags(args, conn, tags, obj_map):
    us = conn.getUpdateService()
    for tag in tags:
        try:
            log.debug('Creating tag: id:%d value:%s',
                      tag.getId(), tag.getValue())

            if get_type_id(tag) in obj_map:
                log.debug('\tAlready in map')
                continue

            if args.n:
                continue

            newTag = omero.model.TagAnnotationI()
            newTag.setTextValue(rstring(tag.getValue()))
            newTag.setNs(rstring(tag.getNs()))
            desc = add_source_text_to_description(tag, tag.getDescription())
            newTag.setDescription(rstring(desc))

            newTag = us.saveAndReturnObject(newTag)
            log.info('Created tag: id:%d value:%s',
                     unwrap(newTag.getId()), unwrap(newTag.getTextValue()))

        except Exception as e:
            log.error('Failed to copy tag [%d] %s: %s',
                      tag.getId(), tag.getTextValue(), e)
            if args.abort:
                raise

def create_tagsets(args, conn, tags, obj_map):
    us = conn.getUpdateService()
    for tag in tags:
        try:
            p = tag.getParent()
            if not p:
                continue

            log.debug('Creating tagset id:%d value:%s parent:%s [%d]',
                      tag.getId(), tag.getValue(),
                      p.OMERO_TYPE.__name__, p.getId())

            if args.n:
                continue

            newP = obj_map[get_type_id(p)]
            newC = obj_map[get_type_id(tag)]

            if newC.getParent() == newP:
                log.debug('Already linked')
                continue

            link = omero.model.AnnotationAnnotationLinkI()
            link.setParent(newP._obj)
            link.setChild(newC._obj)

            link = us.saveAndReturnObject(link)
            log.info('Created tagset: parent:%d %s child:%d %s',
                     unwrap(link.getParent().getId()),
                     unwrap(link.getParent().getTextValue()),
                     unwrap(link.getChild().getId()),
                     unwrap(link.getChild().getTextValue()))

        except Exception as e:
            log.error('Failed to add tag [%d] %s to tagset [%d] %s: %s',
                      tag.getId(), tag.getTextValue(),
                      p.getId(), p.getTextValue(), e)
            if args.abort:
                raise




def main():
    args = parseArgs()
    dryrun = args.n
    conn1 = get_connection(args.ic1)
    conn2 = get_connection(args.ic2)
    tags = get_tags(conn1)

    obj_map = update_obj_map(conn2, 'TagAnnotation')
    create_tags(args, conn2, tags, obj_map)
    obj_map = update_obj_map(conn2, 'TagAnnotation', obj_map)

    create_tagsets(args, conn2, tags, obj_map)

    #conn._closeSession()


if __name__ == '__main__':
    main()
