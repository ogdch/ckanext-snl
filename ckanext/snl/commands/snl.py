import logging
import ckan.lib.cli
import sys

from ckanext.snl.helpers import s3

class SNLCommand(ckan.lib.cli.CkanCommand):
    '''Command to handle snl data

    Usage:

        # Show this help
        paster --plugin=ckanext-snl snl help -c <path to config file>

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        # load pylons config
        self._load_config()
        options = {
                'help': self.helpCmd,
        }

        try:
            cmd = self.args[0]
            options[cmd](*self.args[1:])
        except KeyError:
            self.helpCmd()
            sys.exit(1)

    def helpCmd(self):
        print self.__doc__

