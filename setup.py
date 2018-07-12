from setuptools import setup
setup(name='mysqlslice',
      version='0.1.0.dev1',
      description='update a local databae from a remote one',
      url='https://github.com/mattrixman/mysqlslice',
      author='MatrixmanAtYrService',
      author_email='github@matt.rixman.org', # please use github issues to contact me instead
      packages=['mysqlslice'],
      python_requires= '>=3',
      install_requires=['sh', 'pymysql'],
      entry_points={'console_scripts' : [

          # sync data remote -> local
          'pull_slice = mysqlslice.slice:pull',

          # sync schema remote -> local
          'pull_schema = mysqlslice.schema:pull'

          ]})
