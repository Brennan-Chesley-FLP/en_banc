# Missing Fields: simple_m2m vs search models

Fields from the current search models that would need to be added to each
simplified model entity. Grouped by source model. Fields already present in
the simplified model are omitted.

---

## COURT

Currently has: `id`, `name`, `jurisdiction`, `level`

### From Court
- `short_name` (CharField) — abbreviated court name
- `full_name` (CharField) — full court name (may map to existing `name`)
- `citation_string` (CharField) — Blue Book citation abbreviation
- `url` (URLField) — court homepage
- `start_date` (DateField) — date court established
- `end_date` (DateField) — date court abolished
- `position` (FloatField) — dewey-decimal-style hierarchical ordering
- `in_use` (BooleanField) — whether jurisdiction is active in CourtListener
- `notes` (TextField) — coverage notes
- `pacer_court_id` (SmallIntegerField) — numeric PACER ID
- `pacer_has_rss_feed` (BooleanField) — whether court has PACER RSS
- `pacer_rss_entry_types` (TextField) — types of entries in RSS feed
- `date_last_pacer_contact` (DateTimeField) — last successful PACER contact
- `fjc_court_id` (CharField) — FJC Integrated Database ID
- `has_opinion_scraper` (BooleanField)
- `has_oral_argument_scraper` (BooleanField)

### From Courthouse (1:many from Court, could be a separate entity)
- `building_name` (TextField) — e.g. "John Adams Courthouse"
- `address1`, `address2` (TextField)
- `city`, `county`, `state`, `zip_code`, `country_code` (TextField)
- `court_seat` (BooleanField) — is this the seat of the court

---

## DOCKET

Currently has: `id`, `court_id`, `docket_number`, `opened_date`, `closed_date`

### From Docket
- `source` (SmallIntegerField) — source of the docket data
- `case_name` (TextField) — standard case name
- `case_name_short` (TextField) — abbreviated case name, e.g. "Marsh"
- `case_name_full` (TextField) — full case name
- `slug` (SlugField) — URL slug
- `docket_number_core` (CharField) — distilled federal docket number
- `docket_number_raw` (CharField) — raw docket number from source
- `date_filed` (DateField) — date case was filed
- `date_terminated` (DateField) — date case was terminated
- `date_last_filing` (DateField) — last update to docket
- `date_cert_granted` (DateField)
- `date_cert_denied` (DateField)
- `date_argued` (DateField)
- `date_reargued` (DateField)
- `date_reargument_denied` (DateField)
- `pacer_case_id` (CharField) — PACER case ID
- `cause` (CharField) — cause of action
- `nature_of_suit` (CharField) — NOS code from PACER
- `jury_demand` (CharField)
- `jurisdiction_type` (CharField) — RECAP XML jurisdiction
- `appellate_fee_status` (TextField)
- `appellate_case_type_information` (TextField)
- `mdl_status` (CharField) — MDL status
- `assigned_to_str` (TextField) — assigned judge as text
- `referred_to_str` (TextField) — referred judge as text
- `panel_str` (TextField) — panel judge initials as text
- `appeal_from_str` (TextField) — lower court as text
- `federal_dn_office_code` (CharField) — federal district office code
- `federal_dn_case_type` (CharField) — e.g. cv, mj, cr
- `federal_dn_judge_initials_assigned` (CharField)
- `federal_dn_judge_initials_referred` (CharField)
- `federal_defendant_number` (SmallIntegerField)
- `filepath_local` (FileField) — path to RECAP docket XML
- `filepath_ia` (CharField) — Internet Archive docket path
- `filepath_ia_json` (CharField) — IA JSON path
- `ia_upload_failure_count` (SmallIntegerField)
- `ia_needs_upload` (BooleanField)
- `ia_date_first_change` (DateTimeField)
- `view_count` (IntegerField)
- `blocked` (BooleanField) — blocked from search engine indexing
- `date_blocked` (DateField)
- `date_last_index` (DateTimeField) — last Solr indexing

### From BankruptcyInformation (1:1 with Docket — could be extension fields or separate entity)
- `chapter` (CharField) — bankruptcy chapter
- `trustee_str` (TextField) — trustee name
- `date_converted` (DateTimeField) — chapter conversion date
- `date_last_to_file_claims` (DateTimeField)
- `date_last_to_file_govt` (DateTimeField)
- `date_debtor_dismissed` (DateTimeField)

### From ScotusDocketMetadata (1:1 with Docket — could be extension fields or separate entity)
- `capital_case` (BooleanField)
- `date_discretionary_court_decision` (DateField)
- `linked_with` (CharField) — linked dockets as text
- `questions_presented_url` (CharField)
- `questions_presented_file` (FileField)

### From OriginatingCourtInformation (1:1 with Docket — lower court details)
- `originating_docket_number` (TextField) — lower court docket number
- `originating_assigned_to_str` (TextField) — lower court judge
- `originating_ordering_judge_str` (TextField) — judge who issued final order
- `originating_court_reporter` (TextField)
- `originating_date_disposed` (DateField)
- `originating_date_filed` (DateField)
- `originating_date_judgment` (DateField)
- `originating_date_judgment_eod` (DateField)
- `originating_date_filed_noa` (DateField) — notice of appeal filed
- `originating_date_received_coa` (DateField) — received at court of appeals
- `originating_date_rehearing_denied` (DateField)

### From TrialCourtData (1:1 with Docket — for cases that moved 2+ times)
- `docket_number_trial` (CharField)
- `trial_judge_str` (TextField)
- `trial_reporter` (TextField)
- `trial_date_filed` (DateField)
- `trial_court_name` (TextField)
- `trial_punishment` (TextField) — criminal cases
- `trial_county` (TextField)

---

## DOCKET_EVENT

Currently has: `id`, `docket_id`, `event_type`, `description`, `event_date`

### From DocketEntry
- `time_filed` (TimeField) — time of filing (separate from date)
- `entry_number` (BigIntegerField) — number on PACER docket page
- `recap_sequence_number` (CharField) — CL ordering field
- `pacer_sequence_number` (IntegerField) — PACER de_seqno value

### From SCOTUSDocketEntry
- `sequence_number` (CharField) — CL-generated ordering for SCOTUS entries

---

## PROCEEDING

Currently has: `id`, `case_name`, `status`, `filed_date`

No direct current model equivalent. This is a new concept. Some fields from
Docket/OpinionCluster that are really about the case (not the docket) could
live here:

- `case_name_short` (TextField)
- `case_name_full` (TextField)
- `nature_of_suit` (CharField)
- `cause` (CharField)
- `slug` (SlugField)

---

## DECISION

Currently has: `id`, `citation`, `decided_date`, `disposition`

### From OpinionCluster
- `judges` (TextField) — judge names as text
- `date_filed_is_approximate` (BooleanField)
- `slug` (SlugField)
- `case_name` (TextField)
- `case_name_short` (TextField)
- `case_name_full` (TextField)
- `source` (CharField) — data source
- `procedural_history` (TextField)
- `attorneys` (TextField) — attorneys as free text
- `nature_of_suit` (TextField)
- `posture` (TextField) — procedural posture
- `syllabus` (TextField) — summary of issues and outcome
- `headnotes` (TextField)
- `summary` (TextField)
- `history` (TextField)
- `other_dates` (TextField)
- `cross_reference` (TextField)
- `correction` (TextField) — publisher's correction
- `citation_count` (IntegerField)
- `precedential_status` (CharField) — published, unpublished, etc.
- `blocked` (BooleanField)
- `date_blocked` (DateField)
- `scdb_id` (CharField) — Supreme Court Database ID
- `scdb_decision_direction` (IntegerField) — ideological direction
- `scdb_votes_majority` (IntegerField)
- `scdb_votes_minority` (IntegerField)
- `filepath_json_harvard` (FileField)
- `filepath_pdf_harvard` (FileField)
- `filepath_xml_scan` (FileField)
- `filepath_pdf_scan` (FileField)
- `arguments` (TextField) — attorney arguments as HTML
- `headmatter` (TextField) — content before opinion in Harvard import

### From Citation (1:many from OpinionCluster — should be a separate entity)
The simplified model has a single `citation` string. The current model has a
separate Citation table supporting multiple citations per cluster:
- `volume` (TextField)
- `reporter` (TextField)
- `page` (TextField)
- `type` (SmallIntegerField) — e.g. federal, state, specialty

---

## OPINION

Currently has: `id`, `decision_id`, `author_id`, `opinion_type`, `body`

### From Opinion
- `author_str` (TextField) — author name as text
- `per_curiam` (BooleanField) — no single author
- `joined_by_str` (TextField) — joining judges as text
- `sha1` (CharField) — document hash
- `page_count` (IntegerField)
- `download_url` (URLField) — original source URL
- `local_path` (FileField) — S3 storage path
- `plain_text` (TextField) — extracted plain text
- `html` (TextField) — original HTML
- `html_lawbox` (TextField) — Lawbox HTML
- `html_columbia` (TextField) — Columbia archive HTML
- `html_anon_2020` (TextField) — 2020 anonymous archive HTML
- `xml_harvard` (TextField) — Harvard CAP XML
- `xml_scan` (TextField) — Scanning Project XML
- `html_with_citations` (TextField) — post-processed HTML with citation links
- `extracted_by_ocr` (BooleanField)
- `ordering_key` (IntegerField) — ordering within cluster

### From OpinionContent (1:many from Opinion — multiple content versions)
- `content` (TextField) — text content
- `source` (SmallIntegerField) — content source
- `extraction_type` (SmallIntegerField) — extraction method
- `is_main_version` (BooleanField) — current/official version flag
- `sha1` (CharField)
- `page_count` (IntegerField)
- `download_url` (URLField)
- `local_path` (FileField)

---

## DOCUMENT

Currently has: `id`, `docket_event_id`, `opinion_id`, `resource_type`, `filename`, `original_url`, `local_url`

### From AbstractPDF
- `sha1` (CharField) — document hash / RECAP ID
- `page_count` (IntegerField)
- `file_size` (IntegerField) — size in bytes
- `filepath_ia` (CharField) — Internet Archive URL
- `ia_upload_failure_count` (SmallIntegerField)
- `thumbnail` (FileField)
- `thumbnail_status` (SmallIntegerField)
- `plain_text` (TextField) — extracted text
- `ocr_status` (SmallIntegerField)

### From AbstractPacerDocument / RECAPDocument
- `document_type` (IntegerField) — main document vs attachment
- `document_number` (CharField) — number on docket
- `attachment_number` (SmallIntegerField) — attachment sequence
- `pacer_doc_id` (CharField) — PACER document ID
- `acms_document_guid` (CharField) — ACMS GUID
- `is_available` (BooleanField) — available in RECAP
- `is_free_on_pacer` (BooleanField)
- `is_sealed` (BooleanField)
- `date_upload` (DateTimeField) — RECAP upload date
- `description` (TextField)

### From SCOTUSDocument
- `url` (URLField) — SCOTUS download URL

---

## CROSS_DOCKET_CONCERN

Currently has: `id`, `from_docket_id`, `to_docket_id`, `type`, `description`

### From CaseTransfer
- `transfer_date` (DateField)
- `from_docket_number` (TextField) — for when the docket isn't in the DB
- `to_docket_number` (TextField) — for when the docket isn't in the DB

### From OriginatingCourtInformation (partially — lower court detail that isn't on the docket itself)
- `court_reporter` (TextField)
- `date_disposed` (DateField)
- `date_judgment` (DateField)
- `date_judgment_eod` (DateField)
- `date_filed_noa` (DateField)
- `date_received_coa` (DateField)

---

## OPINION_REFERENCE

Currently has: `id`, `from_opinion_id`, `to_opinion_id`, `type`, `parenthetical`

### From OpinionsCited
- `depth` (IntegerField) — number of times cited

### From Parenthetical
- `score` (FloatField) — how descriptive the parenthetical is (0-1)

### From ParentheticalGroup (aggregation concept — may not need to be modeled)
- `group_score` (FloatField) — quality of the parenthetical group
- `group_size` (IntegerField) — number of parentheticals in group

---

## PERSON

Currently has: `id`, `name`

The current Person model lives in `cl/people_db/` and is extensive. At
minimum, to cover search model string fields that reference people:
- `title` (CharField)
- `name_first`, `name_middle`, `name_last`, `name_suffix` (CharField)

---

## PERSON_COURT

Currently has: `id`, `person_id`, `court_id`, `role`

No additional fields needed — the `role` field covers judge, barred, etc.

---

## PERSON_PROCEEDING

Currently has: `id`, `person_id`, `proceeding_id`, `role`

### From people_db.PartyType (the current Docket↔Party through table)
- `name` (CharField) — the party type name (e.g. "Defendant", "Appellant")
- `extra_info` (TextField) — additional party info
- `highest_offense_level_opening`, `highest_offense_level_terminated` (CharField) — criminal cases

---

## PERSON_DOCUMENT

Currently has: `id`, `person_id`, `document_id`, `role`

No additional fields needed — the `role` field covers author, joining, etc.

---

## COURT_COURT

Currently has: `id`, `from_court_id`, `to_court_id`, `type`

No additional fields needed.

---

## Entities in search models with no simplified model equivalent

### Tag
A cross-cutting label applied via M2M to Docket, DocketEntry, RECAPDocument,
and Claim. Fields: `name` (CharField, unique).

### Claim (bankruptcy-specific)
A creditor's claim on a bankruptcy docket. FK to Docket. Fields:
`claim_number`, `creditor_details`, `creditor_id`, `status`, `entered_by`,
`filed_by`, `amount_claimed`, `unsecured_claimed`, `secured_claimed`,
`priority_claimed`, `description`, `remarks`, plus various dates.

### ClaimHistory (bankruptcy-specific)
Activity log for a Claim. FK to Claim. Inherits AbstractPacerDocument +
AbstractPDF. Fields: `date_filed`, `claim_document_type`, `description`,
`claim_doc_id`, `pacer_dm_id`, `pacer_case_id`.

### SearchQuery
User search tracking. FK to User. Fields: `type`, `query`, `source`,
`engine`, `failed`, `get_params`. Not relevant to the legal data model.

### ClusterRedirection
Tracks merged OpinionClusters. FK to OpinionCluster. Not relevant to the
simplified model unless cluster merging is needed.
