#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from typing import List, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import *

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


class SourceSemrush(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            Projects(api_key=config["api_key"], domain=config["domain"])

            connection = True, None
        except Exception:
            connection = False, None

        return connection

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Backlinks(api_key=config["api_key"], domain=config["domain"]),
            BacklinksOverview(api_key=config["api_key"], domain=config["domain"]),
            ReferringDomains(api_key=config["api_key"], domain=config["domain"]),
            ReferringIps(api_key=config["api_key"], domain=config["domain"]),
            TldDistribution(api_key=config["api_key"], domain=config["domain"]),
            ReferringDomainsByCountry(api_key=config["api_key"], domain=config["domain"]),
            Anchors(api_key=config["api_key"], domain=config["domain"]),
            IndexedPages(api_key=config["api_key"], domain=config["domain"]),
            Competitors(api_key=config["api_key"], domain=config["domain"]),
            AuthorityScoreProfile(api_key=config["api_key"], domain=config["domain"]),
            DomainOrganicSearchKeywords(api_key=config["api_key"], domain=config["domain"]),
            DomainPaidSearchKeywords(api_key=config["api_key"], domain=config["domain"]),
            CompetitorsInOrganicSearch(api_key=config["api_key"], domain=config["domain"]),
            DomainOrganicPages(api_key=config["api_key"], domain=config["domain"]),
            DomainOrganicSubdomains(api_key=config["api_key"], domain=config["domain"]),
            DomainOverviewAllDatabases(api_key=config["api_key"], domain=config["domain"]),
            DomainOverviewOneDatabase(api_key=config["api_key"], domain=config["domain"]),
            DomainOverviewHistory(api_key=config["api_key"], domain=config["domain"]),
        ]
