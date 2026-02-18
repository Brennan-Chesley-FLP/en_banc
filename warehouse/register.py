"""Register scraper output models as warehouse raw tables.

This module is imported by alembic/env.py to ensure all raw table
SQLModel classes are registered on SQLModel.metadata before
autogenerate runs.
"""

from juriscraper.sd.state.alabama.publicportal_alappeals_gov.models import (
    AlaDocket,
    AlaHistoricalReleaseList,
    AlaOpinionCluster,
    AlaOralArgument,
)
from juriscraper.sd.state.connecticut.jud_ct_gov.models import (
    ConnDocket,
    ConnDocketEntry,
    ConnDocketUnavailable,
    ConnOpinionCluster,
    ConnOralArgument,
    ConnTrialCaseUnavailable,
    ConnTrialCourtDocket,
    ConnTrialCourtDocketEntry,
)

from warehouse.raw_tables import raw_table_from_model

# Alabama Public Portal — schema: ala_publicportal
_ALA_SCHEMA = "ala_publicportal"

RawAlaDocket = raw_table_from_model(AlaDocket, _ALA_SCHEMA, "raw_dockets")
RawAlaOpinionCluster = raw_table_from_model(
    AlaOpinionCluster, _ALA_SCHEMA, "raw_opinion_clusters"
)
RawAlaOralArgument = raw_table_from_model(
    AlaOralArgument, _ALA_SCHEMA, "raw_oral_arguments"
)
RawAlaHistoricalReleaseList = raw_table_from_model(
    AlaHistoricalReleaseList, _ALA_SCHEMA, "raw_historical_release_lists"
)

# Connecticut Judicial Branch — schema: conn_jud_ct_gov
_CONN_SCHEMA = "conn_jud_ct_gov"

RawConnDocket = raw_table_from_model(ConnDocket, _CONN_SCHEMA, "raw_dockets")
RawConnDocketEntry = raw_table_from_model(
    ConnDocketEntry, _CONN_SCHEMA, "raw_docket_entries"
)
RawConnOpinionCluster = raw_table_from_model(
    ConnOpinionCluster, _CONN_SCHEMA, "raw_opinion_clusters"
)
RawConnOralArgument = raw_table_from_model(
    ConnOralArgument, _CONN_SCHEMA, "raw_oral_arguments"
)
RawConnDocketUnavailable = raw_table_from_model(
    ConnDocketUnavailable, _CONN_SCHEMA, "raw_docket_unavailable"
)
RawConnTrialCourtDocket = raw_table_from_model(
    ConnTrialCourtDocket, _CONN_SCHEMA, "raw_trial_court_dockets"
)
RawConnTrialCourtDocketEntry = raw_table_from_model(
    ConnTrialCourtDocketEntry, _CONN_SCHEMA, "raw_trial_court_docket_entries"
)
RawConnTrialCaseUnavailable = raw_table_from_model(
    ConnTrialCaseUnavailable, _CONN_SCHEMA, "raw_trial_case_unavailable"
)
