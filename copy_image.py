#!/usr/bin/env python

import logging

import omero
from omero.rtypes import wrap, unwrap, rstring

from copy_utils import update_obj_map, get_type_id, add_source_to_description
from copy_tags import create_tags

#logging.basicConfig()
log = logging.getLogger('OmeroCopy')


def copy_set_get(a, b, include=[], exclude=[]):
    """
    a, b need to be the unwrapped objects
    """
    getMs = {m[3:] for m in dir(a) if m.startswith('get')}
    setMs = {m[3:] for m in dir(b) if m.startswith('set')}
    log.debug('get: %s', getMs)
    log.debug('set: %s', setMs)

    if isinstance(include, str):
        include = [include]
    if isinstance(exclude, str):
        exclude = [exclude]

    union = getMs.union(setMs)
    inter = getMs.intersection(setMs)
    log.debug('inter: %s', inter)

    if include:
        fields = set(include).difference(set(exclude))
    else:
        fields = inter.difference(set(exclude))
        diff = union.difference(inter).difference(set(exclude))
        if 'Details' in diff:
            diff.remove('Details')
        if diff:
            log.error('Mismatch between get and set methods: %s', diff)

    missingGet = fields.difference(getMs)
    missingSet = fields.difference(setMs)
    if missingGet:
        log.error('Fields missing from get: %s', missingGet)
    if missingSet:
        log.error('Fields missing from set: %s', missingSet)

    log.debug('fields: %s', fields)

    for m in fields:
        if m == 'Id':
            continue
        try:
            log.info('Calling %s', m)
            r = getattr(a, 'get' + m)()
            getattr(b, 'set' + m)(r)
        except Exception as e:
            log.error('Call %s failed: %s', m, e)


def copy_image_and_metadata(im, conn):
    """
    Copy an image
    im: The image
    conn: The connection object, possibly on a different server
    """
    sizeZ = im.getSizeZ()
    sizeC = im.getSizeC()
    sizeT = im.getSizeT()
    zctList = [(z,c,t) for z in range(sizeZ) for c in range(sizeC)
               for t in range(sizeT)]

    def planeGen():
        planes = im.getPrimaryPixels().getPlanes(zctList)
        for p in planes:
            yield p

    d = im.getDescription()
    newim = conn.createImageFromNumpySeq(
        planeGen(), im.getName(), sizeZ=sizeZ, sizeC=sizeC, sizeT=sizeT)

    #qs = conn.getQueryService()
    #px = qs.get('Pixels', newim.getPrimaryPixels().id)

    #params = omero.sys.Parameters()
    #params.map = { 'id': wrap(px.id) }
    #channels = qs.findAllByQuery(
    #    "SELECT c from Channel c "
    #    "join fetch c.pixels as p "
    #    "where p.id = :id", params)

    us = conn.getUpdateService()

    px_exc = [
        'AnnotationLinksCountPerOwner',
        'Channel',
        'Image',
        'PixelsFileMapsCountPerOwner',
        'PixelsType',
        'PrimaryChannel',
        'RelatedTo',
        'Sha1',
        'SizeC',
        'SizeT',
        'SizeX',
        'SizeY',
        'SizeZ',
        ]

    newim = conn.getObject('Image', newim.id)
    newpx = newim.getPrimaryPixels()
    copy_set_get(im.getPrimaryPixels()._obj, newpx._obj, exclude=px_exc)
    newpx = us.saveAndReturnObject(newpx._obj)

    ch_exc = [
        'AnnotationLinksCountPerOwner',
        'LogicalChannel',
        'Pixels',
        'StatsInfo',
        ]
    lc_exc = ['DetectorSettings']

    for c in xrange(sizeC):
        newim = conn.getObject('Image', newim.id)
        chsrc = im.getChannels()[c]
        chdst = newim.getChannels()[c]
        copy_set_get(chsrc._obj, chdst._obj, exclude=ch_exc)
        us.saveAndReturnObject(chdst._obj)

        lchsrc = chsrc.getLogicalChannel()
        lchdst = chdst.getLogicalChannel()
        copy_set_get(lchsrc._obj, lchdst._obj, exclude=lc_exc)
        us.saveAndReturnObject(lchdst._obj)

    desc = add_source_to_description(im, im.getDescription())
    newim.setDescription(rstring(desc))
    us.saveAndReturnObject(newim._obj)

    return newim


def copy_tags(im, newim, conn, obj_map):
    us = conn.getUpdateService()
    tags = [a for a in im.listAnnotations()
            if isinstance(a._obj, omero.model.TagAnnotationI)]

    obj_map = update_obj_map(conn, 'TagAnnotation')
    create_tags(None, conn, tags, obj_map)
    obj_map = update_obj_map(conn, 'TagAnnotation')

    newim = conn.getObject('Image', newim.id)

    for tag in tags:
        link = omero.model.ImageAnnotationLinkI()
        link.setParent(newim._obj)
        link.setChild(obj_map[get_type_id(tag)]._obj)
        link = us.saveAndReturnObject(link)


