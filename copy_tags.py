#!/usr/bin/env python

import logging

import omero
from omero.rtypes import wrap, unwrap, rstring

from copy_utils import update_obj_map, get_type_id, add_source_to_description

logging.basicConfig()
log = logging.getLogger('OmeroCopy')


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

def create_tags(args, conn, tags, obj_map):
    us = conn.getUpdateService()
    log.debug('create_tags: %s', obj_map)
    for tag in tags:
        try:
            log.debug('Creating tag: id:%d value:%s',
                      tag.getId(), tag.getValue())

            if get_type_id(tag) in obj_map:
                log.debug('\tAlready in map')
                continue
            log.debug('type_id: %s', get_type_id(tag))

            if args and args.n:
                continue

            newTag = omero.model.TagAnnotationI()
            newTag.setTextValue(rstring(tag.getValue()))
            newTag.setNs(rstring(tag.getNs()))
            desc = add_source_to_description(tag, tag.getDescription())
            newTag.setDescription(rstring(desc))

            newTag = us.saveAndReturnObject(newTag)
            log.info('Created tag: id:%d value:%s',
                     unwrap(newTag.getId()), unwrap(newTag.getTextValue()))

        except Exception as e:
            log.error('Failed to copy tag [%d] %s: %s',
                      tag.getId(), tag.getTextValue(), e)
            if args and args.abort:
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

            if args and args.n:
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
            if args and args.abort:
                raise

