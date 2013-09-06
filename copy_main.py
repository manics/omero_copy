#!/usr/bin/env python

import argparse
import logging

import omero
import omero.gateway

from copy_utils import update_obj_map, get_type_id, add_source_to_description
from copy_tags import get_tags, create_tags, create_tagsets


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
