'''
Usage: python -m gitlabhammer [options] <URL> [project] [git-hammer add-repository options]
 -j --jobs=         Number of parallel jobs
 -u --update        Update existing Git clones (default)
 -U --no-update     Do not update existing Git clones
 -s --sync          Sync Gitlab project list to Git Hammer (default)
 -S --no-sync       Do not sync Gitlab project list to Git Hammer
 <URL>              Gitlab instance
 [project]          Git Hammer project name
'''

import multiprocessing.pool
import os
import os.path
import subprocess
import sys
from urllib.parse import urljoin, urlparse

from githammer import hammer
import requests


def usage(exitcode=1):
    print(__doc__, end='', file=sys.stderr, flush=True)
    sys.exit(exitcode)


jobs, update, sync = None, True, True
args, shift = sys.argv[1:], 0
try:
    while shift < 2 and len(args) > shift:
        arg = args[shift]
        if arg.startswith('-j'):
            args.pop(shift)
            jobs = int(arg[2:] or args.pop(shift))
        elif arg.startswith('--jobs='):
            args.pop(shift)
            jobs = int(arg[7:])
        elif arg == '--jobs':
            args.pop(shift)
            jobs = int(args.pop(shift))
        if arg in ('-u', '--update'):
            args.pop(shift)
            update = True
        elif arg in ('-U', '--no-update'):
            args.pop(shift)
            update = False
        elif arg in ('-s', '--sync'):
            args.pop(shift)
            sync = True
        elif arg in ('-S', '--no-sync'):
            args.pop(shift)
            sync = False
        elif arg in ('-?', '-h', '--help', '--usage'):
            args.pop(shift)
            usage(exitcode=0)
        elif arg.startswith('-'):
            if arg == '--':
                args.pop(shift)
            break
        else:
            shift += 1
    url = args.pop(0)
    url = urlparse(url if '//' in url else f"//{url}", scheme='https')
    projectname = args.pop(
        0) if args and not args[0].startswith('-') else url.netloc
except IndexError as _:
    usage()

database_url = os.environ.get('DATABASE_URL')
try:
    existing = set(
        hammer.iter_all_project_names(database_url=database_url)
        if database_url else hammer.iter_all_project_names())
except hammer.DatabaseNotInitializedError as _:
    existing = set()
projects = {name: {'fullPath': name} for name in existing}

if sync:
    api, cursor, page, count = urljoin(url.geturl(), 'api/graphql'), None, 0, 0
    while True:
        r = requests.post(api,
                          json={
                              'query': '''query Projects($cursor: String) {
  projects(after: $cursor) {
    nodes {
      nameWithNamespace
      fullPath
      httpUrlToRepo
      sshUrlToRepo
    }
    pageInfo {
      endCursor
    }
  }
}''',
                              'variables': {
                                  'cursor': cursor
                              }
                          })
        r.raise_for_status()
        r = r.json()
        if 'errors' in r:
            raise RuntimeError(r['errors'])
        r = r.get('data', {}).get('projects', {})
        page += 1
        count += len(r.get('nodes', []))
        print(f"page {page} count {count}\r",
              end='',
              file=sys.stderr,
              flush=True)
        projects.update(
            (project['fullPath'], project) for project in r.get('nodes', []))
        cursor = r.get('pageInfo', {}).get('endCursor')
        if cursor is None:
            break
    print(file=sys.stderr, flush=True)


def clone(project):
    name = project['fullPath']
    repoUrl = project.get('httpUrlToRepo') or project.get('sshUrlToRepo')
    print(f"git fetch {project.get('nameWithNamespace', name)}")
    path = os.path.join(projectname, name)
    exists = subprocess.run(['git', 'rev-parse', '--is-inside-git-dir'],
                            stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE,
                            cwd=path,
                            check=True,
                            text=True).stdout.rstrip() if os.access(
                                path, os.F_OK) else 'false'
    if exists == 'false':
        if not repoUrl:
            return
        os.makedirs(path, exist_ok=True)
        subprocess.run(['git', 'init', '--bare'],
                       stdin=subprocess.DEVNULL,
                       cwd=path,
                       check=True)
    elif exists != 'true':
        raise TypeError(f"not a boolean: {exists}")
    if repoUrl:
        proc = subprocess.run(['git', 'remote', 'get-url', 'origin'],
                              stdin=subprocess.DEVNULL,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL,
                              cwd=path,
                              text=True)
        if proc.returncode:
            subprocess.run(['git', 'remote', 'add', 'origin', repoUrl],
                           stdin=subprocess.DEVNULL,
                           cwd=path,
                           check=True)
        elif proc.stdout.rstrip() != repoUrl:
            subprocess.run(['git', 'remote', 'set-url', 'origin', repoUrl],
                           stdin=subprocess.DEVNULL,
                           cwd=path,
                           check=True)
    if update:
        subprocess.run(
            ['git', 'fetch', '-n', repoUrl or 'origin', 'HEAD:HEAD'],
            stdin=subprocess.DEVNULL,
            cwd=path,
            check=True)
    return name


with multiprocessing.pool.Pool(jobs) as pool:
    for name in pool.imap_unordered(clone,
                                    (project
                                     for name, project in projects.items()
                                     if update or name not in existing)):
        if name not in existing:
            subprocess.run([
                sys.executable, '-m', 'githammer', 'add-repository',
                projectname,
                os.path.join(projectname, name), *args
            ],
                           stdin=subprocess.DEVNULL,
                           check=True)
