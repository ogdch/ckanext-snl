import logging
import ckan.lib.cli
import sys

from ckanext.snl.helpers import oai

class SNLCommand(ckan.lib.cli.CkanCommand):
    '''Command to handle snl data

    Usage:

        # Show this help
        paster --plugin=ckanext-snl snl help -c <path to config file>

        # Export the oai entries for the specified set
        paster --plugin=ckanext-snl snl export <set name> -c <path to config file>

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        # load pylons config
        self._load_config()
        options = {
                'help': self.helpCmd,
                'export': self.exportCmd,
                'resume': self.resumeCmd,
                'dump': self.dumpCmd
        }

        try:
            cmd = self.args[0]
            options[cmd](*self.args[1:])
        except KeyError, e:
            print e
            self.helpCmd()
            sys.exit(1)

    def helpCmd(self):
        print self.__doc__

    def exportCmd(self, set_name):
        oai_helper = oai.OAI('ch.nb')
        append = True if set_name == 'NewBib' else False
        oai_helper.export(set_name, append)

    def resumeCmd(self, set_name, count, limit=None):
        oai_helper = oai.OAI('ch.nb')
        append = True if set_name == 'NewBib' else False
        count = int(count)
        try:
            limit = int(limit)
        except ValueError:
            limit = None
        oai_helper.resume_export(set_name, append, count, limit)

    def dumpCmd(self, set_name):
        oai_helper = oai.OAI('ch.nb')
        oai_helper.dump(set_name)
