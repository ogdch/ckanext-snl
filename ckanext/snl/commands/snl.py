import ckan.lib.cli
import sys

from ckanext.snl.helpers import oai


class SNLCommand(ckan.lib.cli.CkanCommand):
    '''Command to handle snl data

    Usage:

        # General usage
        paster --plugin=ckanext-snl <command> -c <path to config file>

        # Show this help
        paster snl help

        # Export the oai entries for the specified set
        paster snl export <set name>

        # Resume export of the oai entries for the specified set
        paster snl resume <set name> <start record count> <limit record count>

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    APPEND_SETS = [
        'NewBib',
        'sb'
    ]

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

    def exportCmd(
            self,
            set_name,
            oai_url='http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'):
        oai_helper = oai.OAI('ch.nb', oai_url)
        append = True if set_name == 'NewBib' else False
        print oai_helper.export(set_name, append)

    def resumeCmd(
            self,
            set_name,
            count,
            limit=None,
            oai_url='http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'):
        oai_helper = oai.OAI('ch.nb', oai_url)
        append = True if set_name in self.APPEND_SETS else False
        count = int(count)
        try:
            limit = int(limit)
        except ValueError:
            limit = None
        print oai_helper.resume_export(set_name, append, count, limit)

    def dumpCmd(self, set_name):
        oai_helper = oai.OAI('ch.nb')
        oai_helper.dump(set_name)
