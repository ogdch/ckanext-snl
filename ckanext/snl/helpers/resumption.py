from oaipmh import client, validation
from oaipmh.datestamp import datetime_to_datestamp


class ResumptionClient(client.Client):
    def __init__(
            self,
            base_url,
            metadataPrefix='oai_dc',
            metadata_registry=None,
            credentials=None):
        client.Client.__init__(self, base_url, metadata_registry, credentials)
        self.metadataPrefix = metadataPrefix

    def handleVerb(self, verb, kw):
        # validate kw first
        validation.validateResumptionArguments(verb, kw)
        # encode datetimes as datestamps
        from_ = kw.get('from_')
        if from_ is not None:
            # turn it into 'from', not 'from_' before doing actual request
            kw['from'] = datetime_to_datestamp(from_,
                                               self._day_granularity)
        if 'from_' in kw:
            # always remove it from the kw, no matter whether it be None or not
            del kw['from_']

        until = kw.get('until')
        if until is not None:
            kw['until'] = datetime_to_datestamp(until,
                                                self._day_granularity)
        elif 'until' in kw:
            # until is None but is explicitly in kw, remove it
            del kw['until']

        # now call underlying implementation
        method_name = verb + '_impl'
        return getattr(self, method_name)(
            kw, self.makeRequestErrorHandling(verb=verb, **kw))

    def ListRecords_impl(self, args, tree):
        namespaces = self.getNamespaces()
        metadata_prefix = args.get('metadataPrefix', self.metadataPrefix)
        metadata_registry = self._metadata_registry

        def firstBatch():
            return self.buildRecords(
                metadata_prefix, namespaces,
                metadata_registry, tree)

        def nextBatch(token):
            tree = self.makeRequestErrorHandling(
                verb='ListRecords',
                resumptionToken=token)
            return self.buildRecords(
                metadata_prefix, namespaces,
                metadata_registry, tree)
        return client.ResumptionListGenerator(firstBatch, nextBatch)
