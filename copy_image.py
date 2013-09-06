#!/usr/bin/env python

import logging

import omero


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
        diff = union.difference(inter)
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


def copy_image(im, conn):
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

    newim = conn.createImageFromNumpySeq(
        planeGen(), im.getName(), sizeZ=sizeZ, sizeC=sizeC, sizeT=sizeT)
    return newim

def copy_logical_channel(src, conn):
    """
    Copy and save a logical channel
    src: The logical channel
    conn: The connection object, possibly on a different server
    """
    dst = omero.model.LogicalChannelI()
    exc = ['DetectorSettings']
    copy_set_get(src, dst, exclude=exc)
    dst = conn.getUpdateService().saveAndReturnObject(dst)
    return dst

def copy_channel(src, conn):
    """
    Copy and save a channel
    src: The channel
    conn: The connection object, possibly on a different server
    """
    dst = omero.model.ChannelI()
    exc = ['AnnotationLinksCountPerOwner', 'LogicalChannel', 'Pixels']
    copy_set_get(src, dst, exclude=exc)
    dstlc = copy_logical_channel(src.getLogicalChannel(), conn)
    dst.setLogicalChannel(dstlc)
    # Currently doesn't work
    dst = conn.getUpdateService().saveAndReturnObject(dst)
    return dst

