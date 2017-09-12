#!/usr/bin/env python


if __name__ == '__main__':
    import os
    import subprocess
    import tempfile

    tmpdir = tempfile.mkdtemp()
    subprocess.check_call([
        'git',
        'clone',
        '--quiet',
        '--depth',
        '1',
        'git://github.com/apache/arrow',
        tmpdir,
    ])
    os.chdir(tmpdir)
    out = subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    print(out.decode('utf8'))
