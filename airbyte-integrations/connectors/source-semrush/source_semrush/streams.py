from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream

from .utils import csv_to_json


class SemrushStream(HttpStream, ABC):
    def __init__(self, api_key, domain, *args):
        super().__init__(*args)
        self.api_key = api_key
        self.domain = domain

    url_base = "https://api.semrush.com"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {"key": self.api_key}


# Basic incremental stream
class IncrementalSemrushStream(SemrushStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return ""

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Backlinks(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "page_ascore,source_title,source_url,target_url,anchor,external_num,internal_num,first_seen,last_seen",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return csv_to_json(response.text)


class BacklinksOverview(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_overview",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "ascore,total,domains_num,urls_num,ips_num,ipclassc_num,follows_num,nofollows_num,"
                              "sponsored_num,ugc_num,texts_num,images_num,forms_num,frames_num",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return csv_to_json(response.text)


class ReferringDomains(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_refdomains",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "domain_ascore,domain,backlinks_num,ip,country,first_seen,last_seen",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return csv_to_json(response.text)


class ReferringIps(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_refips",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "ip,country,domains_num,backlinks_num,first_seen,last_seen",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class TldDistribution(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_tld",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "zone,domains_num,backlinks_num",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class ReferringDomainsByCountry(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_geo",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "country,domains_num,backlinks_num",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class Anchors(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_anchors",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "anchor,domains_num,backlinks_num,first_seen,last_seen",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class IndexedPages(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_pages",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "source_url,source_title,response_code,backlinks_num,domains_num,last_seen,external_num,internal_num",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class Competitors(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_competitors",
            "target": "xon.so",
            "target_type": "root_domain",
            "export_columns": "ascore,neighbour,similarity,common_refdomains,domains_num,backlinks_num",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class AuthorityScoreProfile(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "/analytics/v1"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "backlinks_ascore_profile",
            "target": "arbisoft.com",
            "target_type": "root_domain",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainOrganicSearchKeywords(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_organic",
            "domain": "seobook.com",
            "display_filter": "%2B%7CPh%7CCo%7Cseo",
            "export_columns": "Ph,Po,Pp,Pd,Nq,Cp,Ur,Tr,Tc,Co,Nr,Td",
            "display_limit": "1",
            "database": "us",
            "display_sort": "tr_desc"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainPaidSearchKeywords(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_adwords",
            "domain": "ebay.com",
            "export_columns": "Ph,Po,Pp,Pd,Nq,Cp,Vu,Tr,Tc,Co,Nr,Td",
            "display_limit": "1",
            "database": "us",
            "display_sort": "po_asc"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class CompetitorsInOrganicSearch(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_organic_organic",
            "domain": "seobook.com",
            "export_columns": "Dn,Cr,Np,Or,Ot,Oc,Ad",
            "display_limit": "1",
            "database": "us"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainOrganicPages(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_organic_unique",
            "display_filter": "%2B%7CPc%7CGt%7C100",
            "domain": "seobook.com",
            "export_columns": "Ur,Pc,Tg,Tr",
            "display_limit": "1",
            "database": "us",
            "display_sort": "tr_desc",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainOrganicSubdomains(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_organic_subdomains",
            "domain": "apple.com",
            "export_columns": "Ur,Pc,Tg,Tr",
            "display_limit": "1",
            "database": "us"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainOverviewAllDatabases(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_ranks",
            "domain": "apple.com",
            "export_columns": "Db,Dn,Rk,Or,Ot,Oc,Ad,At,Ac,Sh,Sv",
            "display_limit": "1",
            "database": "us"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainOverviewOneDatabase(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_rank",
            "domain": "seobook.com",
            "export_columns": "Dn,Rk,Or,Ot,Oc,Ad,At,Ac",
            "display_limit": "1",
            "database": "us"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainOverviewHistory(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return self.url_base

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "type": "domain_rank_history",
            "domain": "ebay.com",
            "export_columns": "Rk,Or,Ot,Oc,Ad,At,Ac,Dt",
            "display_limit": "1",
            "database": "us"
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class TrafficSummary(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "analytics/ta/api/v3/summary"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "targets": "golang.org,blog.golang.org,tour.golang.org/welcome/",
            "export_columns": "target,visits,users",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class TopPages(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "analytics/ta/api/v3/summary"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "device_type": "desktop",
            "display_date": "2020-06-01",
            "country": "us",
            "target": "amazon.com",
            "target_type": "domain",
            "export_columns": "page,display_date,desktop_share,mobile_share",
            "display_limit": "1",
            "display_offset": "0",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DomainRankings(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "analytics/ta/api/v3/summary"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "device_type": "mobile",
            "display_date": "2020-05-01",
            "country": "us",
            "export_columns": "rank,domain",
            "display_limit": "1",
            "display_offset": "0",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class AudienceInsights(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "analytics/ta/api/v3/summary"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "device_type": "desktop",
            "display_date": "2020-02-01",
            "country": "us",
            "segment": "contains",
            "targets": "amazon.com,ebay.com,searchenginesland.com",
            "selected_targets": "amazon.com,ebay.com",
            "export_columns": "target,overlap_score,similarity_score,target_users,overlap_users",
            "display_limit": "1",
            "display_offset": "0",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}


class DataAccuracy(SemrushStream):
    primary_key = "page_source"
    cursor_field = "page_source"

    def path(self, **kwargs) -> str:
        return "analytics/ta/api/v3/summary"

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "key": self.api_key,
            "display_date": "2019-01-01",
            "target": "ebay.com",
            "country": "us",
            "device_type": "desktop",
            "export_columns": "target,display_date,country,device_type,accuracy",
            "display_limit": "1",
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return {}
