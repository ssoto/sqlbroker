import os

import setuptools

from pip import download
from pip import req


HERE = os.path.abspath(os.path.dirname(__file__))


def get_requirements(reqfile):
    path = os.path.join(HERE, reqfile)
    deps = set()
    for dep in req.parse_requirements(path, session=download.PipSession()):
        specs = ','.join(''.join(spec) for spec in dep.req.specs)
        requirement = '{name}{extras}{specs}'.format(
            name=dep.name,
            extras=(
                '[{extras}]'.format(extras=','.join(dep.extras))
                if dep.extras else ''
            ),
            specs=specs,
        )

        deps.add(requirement)
    return deps


setuptools.setup(
    name='sqlbroker',
    description='WTelecom SQL wrapper',
    version=':versiontools:sqlbroker:',

    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=get_requirements('requirements/requirements.txt'),
    tests_require=get_requirements('requirements/test-requirements.txt'),
    setup_requires=('versiontools'),

    author='WTelecom DevTeam',
    author_email='dev@wtelecom.com',
    url='http://www.wtelecom.es',
)
