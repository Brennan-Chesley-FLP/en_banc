.. md-mermaid::

   graph LR
       subgraph ala_publicportal
           ala_publicportal_corrections_dockets[corrections_dockets]
           ala_publicportal_corrections_opinion_clusters[corrections_opinion_clusters]
           ala_publicportal_corrections_oral_arguments[corrections_oral_arguments]
           ala_publicportal_irr_future_dates_opinion_clusters[irr_future_dates_opinion_clusters]
           ala_publicportal_irr_invalid_court_ids_dockets[irr_invalid_court_ids_dockets]
           ala_publicportal_latest_dockets[latest_dockets]
           ala_publicportal_latest_historical_release_lists[latest_historical_release_lists]
           ala_publicportal_latest_opinion_clusters[latest_opinion_clusters]
           ala_publicportal_latest_oral_arguments[latest_oral_arguments]
           ala_publicportal_raw_dockets[raw_dockets]
           ala_publicportal_raw_dockets_observations[raw_dockets_observations]
           ala_publicportal_raw_historical_release_lists[raw_historical_release_lists]
           ala_publicportal_raw_historical_release_lists_observations[raw_historical_release_lists_observations]
           ala_publicportal_raw_opinion_clusters[raw_opinion_clusters]
           ala_publicportal_raw_opinion_clusters_observations[raw_opinion_clusters_observations]
           ala_publicportal_raw_oral_arguments[raw_oral_arguments]
           ala_publicportal_raw_oral_arguments_observations[raw_oral_arguments_observations]
           ala_publicportal_staged_docket_entries[staged_docket_entries]
           ala_publicportal_staged_docket_parties[staged_docket_parties]
           ala_publicportal_staged_dockets[staged_dockets]
           ala_publicportal_staged_historical_release_lists[staged_historical_release_lists]
           ala_publicportal_staged_opinion_clusters[staged_opinion_clusters]
           ala_publicportal_staged_opinions[staged_opinions]
           ala_publicportal_staged_oral_arguments[staged_oral_arguments]
       end
       subgraph conn_jud_ct_gov
           conn_jud_ct_gov_corrections_docket_entries[corrections_docket_entries]
           conn_jud_ct_gov_corrections_dockets[corrections_dockets]
           conn_jud_ct_gov_corrections_opinion_clusters[corrections_opinion_clusters]
           conn_jud_ct_gov_corrections_oral_arguments[corrections_oral_arguments]
           conn_jud_ct_gov_irr_future_dates_opinion_clusters[irr_future_dates_opinion_clusters]
           conn_jud_ct_gov_irr_invalid_court_ids_dockets[irr_invalid_court_ids_dockets]
           conn_jud_ct_gov_latest_docket_entries[latest_docket_entries]
           conn_jud_ct_gov_latest_dockets[latest_dockets]
           conn_jud_ct_gov_latest_opinion_clusters[latest_opinion_clusters]
           conn_jud_ct_gov_latest_oral_arguments[latest_oral_arguments]
           conn_jud_ct_gov_raw_docket_entries[raw_docket_entries]
           conn_jud_ct_gov_raw_docket_entries_observations[raw_docket_entries_observations]
           conn_jud_ct_gov_raw_dockets[raw_dockets]
           conn_jud_ct_gov_raw_dockets_observations[raw_dockets_observations]
           conn_jud_ct_gov_raw_opinion_clusters[raw_opinion_clusters]
           conn_jud_ct_gov_raw_opinion_clusters_observations[raw_opinion_clusters_observations]
           conn_jud_ct_gov_raw_oral_arguments[raw_oral_arguments]
           conn_jud_ct_gov_raw_oral_arguments_observations[raw_oral_arguments_observations]
           conn_jud_ct_gov_staged_docket_entries[staged_docket_entries]
           conn_jud_ct_gov_staged_docket_parties[staged_docket_parties]
           conn_jud_ct_gov_staged_dockets[staged_dockets]
           conn_jud_ct_gov_staged_opinion_clusters[staged_opinion_clusters]
           conn_jud_ct_gov_staged_opinions[staged_opinions]
           conn_jud_ct_gov_staged_oral_arguments[staged_oral_arguments]
       end
       subgraph corrections
           corrections_corrections[corrections]
       end
       subgraph courtlistener
           courtlistener_corrections_audio[corrections_audio]
           courtlistener_corrections_docket_entries[corrections_docket_entries]
           courtlistener_corrections_dockets[corrections_dockets]
           courtlistener_corrections_opinion_clusters[corrections_opinion_clusters]
           courtlistener_corrections_opinions[corrections_opinions]
           courtlistener_corrections_originating_court_information[corrections_originating_court_information]
           courtlistener_irr_future_dates_opinion_clusters[irr_future_dates_opinion_clusters]
           courtlistener_irr_invalid_court_ids_dockets[irr_invalid_court_ids_dockets]
           courtlistener_irr_null_docket_numbers_dockets[irr_null_docket_numbers_dockets]
           courtlistener_raw_audio[raw_audio]
           courtlistener_raw_docket_entries[raw_docket_entries]
           courtlistener_raw_dockets[raw_dockets]
           courtlistener_raw_opinion_clusters[raw_opinion_clusters]
           courtlistener_raw_opinions[raw_opinions]
           courtlistener_raw_originating_court_information[raw_originating_court_information]
           courtlistener_staged_audio[staged_audio]
           courtlistener_staged_docket_entries[staged_docket_entries]
           courtlistener_staged_dockets[staged_dockets]
           courtlistener_staged_opinion_clusters[staged_opinion_clusters]
           courtlistener_staged_opinions[staged_opinions]
           courtlistener_staged_originating_court_information[staged_originating_court_information]
       end
       subgraph warehouse
           warehouse_court_ids[court_ids]
           warehouse_provenance[provenance]
       end
   
       ala_publicportal_corrections_dockets --> ala_publicportal_irr_invalid_court_ids_dockets
       ala_publicportal_corrections_dockets --> ala_publicportal_staged_dockets
       ala_publicportal_corrections_dockets --> courtlistener_irr_invalid_court_ids_dockets
       ala_publicportal_corrections_dockets --> courtlistener_irr_null_docket_numbers_dockets
       ala_publicportal_corrections_dockets --> courtlistener_raw_dockets
       ala_publicportal_corrections_dockets --> courtlistener_raw_originating_court_information
       ala_publicportal_corrections_dockets --> courtlistener_staged_dockets
       ala_publicportal_corrections_dockets --> courtlistener_staged_originating_court_information
       ala_publicportal_corrections_opinion_clusters --> ala_publicportal_irr_future_dates_opinion_clusters
       ala_publicportal_corrections_opinion_clusters --> ala_publicportal_staged_opinion_clusters
       ala_publicportal_corrections_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       ala_publicportal_corrections_opinion_clusters --> courtlistener_raw_opinion_clusters
       ala_publicportal_corrections_opinion_clusters --> courtlistener_staged_opinion_clusters
       ala_publicportal_corrections_oral_arguments --> ala_publicportal_staged_oral_arguments
       ala_publicportal_corrections_oral_arguments --> courtlistener_raw_audio
       ala_publicportal_corrections_oral_arguments --> courtlistener_staged_audio
       ala_publicportal_latest_dockets --> ala_publicportal_irr_invalid_court_ids_dockets
       ala_publicportal_latest_dockets --> ala_publicportal_staged_docket_entries
       ala_publicportal_latest_dockets --> ala_publicportal_staged_docket_parties
       ala_publicportal_latest_dockets --> ala_publicportal_staged_dockets
       ala_publicportal_latest_dockets --> courtlistener_irr_invalid_court_ids_dockets
       ala_publicportal_latest_dockets --> courtlistener_irr_null_docket_numbers_dockets
       ala_publicportal_latest_dockets --> courtlistener_raw_docket_entries
       ala_publicportal_latest_dockets --> courtlistener_raw_dockets
       ala_publicportal_latest_dockets --> courtlistener_raw_originating_court_information
       ala_publicportal_latest_dockets --> courtlistener_staged_docket_entries
       ala_publicportal_latest_dockets --> courtlistener_staged_dockets
       ala_publicportal_latest_dockets --> courtlistener_staged_originating_court_information
       ala_publicportal_latest_historical_release_lists --> ala_publicportal_staged_historical_release_lists
       ala_publicportal_latest_opinion_clusters --> ala_publicportal_irr_future_dates_opinion_clusters
       ala_publicportal_latest_opinion_clusters --> ala_publicportal_staged_opinion_clusters
       ala_publicportal_latest_opinion_clusters --> ala_publicportal_staged_opinions
       ala_publicportal_latest_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       ala_publicportal_latest_opinion_clusters --> courtlistener_raw_opinion_clusters
       ala_publicportal_latest_opinion_clusters --> courtlistener_raw_opinions
       ala_publicportal_latest_opinion_clusters --> courtlistener_staged_opinion_clusters
       ala_publicportal_latest_opinion_clusters --> courtlistener_staged_opinions
       ala_publicportal_latest_oral_arguments --> ala_publicportal_staged_oral_arguments
       ala_publicportal_latest_oral_arguments --> courtlistener_raw_audio
       ala_publicportal_latest_oral_arguments --> courtlistener_staged_audio
       ala_publicportal_raw_dockets --> ala_publicportal_irr_invalid_court_ids_dockets
       ala_publicportal_raw_dockets --> ala_publicportal_latest_dockets
       ala_publicportal_raw_dockets --> ala_publicportal_staged_docket_entries
       ala_publicportal_raw_dockets --> ala_publicportal_staged_docket_parties
       ala_publicportal_raw_dockets --> ala_publicportal_staged_dockets
       ala_publicportal_raw_dockets --> courtlistener_irr_invalid_court_ids_dockets
       ala_publicportal_raw_dockets --> courtlistener_irr_null_docket_numbers_dockets
       ala_publicportal_raw_dockets --> courtlistener_raw_docket_entries
       ala_publicportal_raw_dockets --> courtlistener_raw_dockets
       ala_publicportal_raw_dockets --> courtlistener_raw_originating_court_information
       ala_publicportal_raw_dockets --> courtlistener_staged_docket_entries
       ala_publicportal_raw_dockets --> courtlistener_staged_dockets
       ala_publicportal_raw_dockets --> courtlistener_staged_originating_court_information
       ala_publicportal_raw_dockets_observations --> ala_publicportal_irr_invalid_court_ids_dockets
       ala_publicportal_raw_dockets_observations --> ala_publicportal_latest_dockets
       ala_publicportal_raw_dockets_observations --> ala_publicportal_staged_docket_entries
       ala_publicportal_raw_dockets_observations --> ala_publicportal_staged_docket_parties
       ala_publicportal_raw_dockets_observations --> ala_publicportal_staged_dockets
       ala_publicportal_raw_dockets_observations --> courtlistener_irr_invalid_court_ids_dockets
       ala_publicportal_raw_dockets_observations --> courtlistener_irr_null_docket_numbers_dockets
       ala_publicportal_raw_dockets_observations --> courtlistener_raw_docket_entries
       ala_publicportal_raw_dockets_observations --> courtlistener_raw_dockets
       ala_publicportal_raw_dockets_observations --> courtlistener_raw_originating_court_information
       ala_publicportal_raw_dockets_observations --> courtlistener_staged_docket_entries
       ala_publicportal_raw_dockets_observations --> courtlistener_staged_dockets
       ala_publicportal_raw_dockets_observations --> courtlistener_staged_originating_court_information
       ala_publicportal_raw_historical_release_lists --> ala_publicportal_latest_historical_release_lists
       ala_publicportal_raw_historical_release_lists --> ala_publicportal_staged_historical_release_lists
       ala_publicportal_raw_historical_release_lists_observations --> ala_publicportal_latest_historical_release_lists
       ala_publicportal_raw_historical_release_lists_observations --> ala_publicportal_staged_historical_release_lists
       ala_publicportal_raw_opinion_clusters --> ala_publicportal_irr_future_dates_opinion_clusters
       ala_publicportal_raw_opinion_clusters --> ala_publicportal_latest_opinion_clusters
       ala_publicportal_raw_opinion_clusters --> ala_publicportal_staged_opinion_clusters
       ala_publicportal_raw_opinion_clusters --> ala_publicportal_staged_opinions
       ala_publicportal_raw_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       ala_publicportal_raw_opinion_clusters --> courtlistener_raw_opinion_clusters
       ala_publicportal_raw_opinion_clusters --> courtlistener_raw_opinions
       ala_publicportal_raw_opinion_clusters --> courtlistener_staged_opinion_clusters
       ala_publicportal_raw_opinion_clusters --> courtlistener_staged_opinions
       ala_publicportal_raw_opinion_clusters_observations --> ala_publicportal_irr_future_dates_opinion_clusters
       ala_publicportal_raw_opinion_clusters_observations --> ala_publicportal_latest_opinion_clusters
       ala_publicportal_raw_opinion_clusters_observations --> ala_publicportal_staged_opinion_clusters
       ala_publicportal_raw_opinion_clusters_observations --> ala_publicportal_staged_opinions
       ala_publicportal_raw_opinion_clusters_observations --> courtlistener_irr_future_dates_opinion_clusters
       ala_publicportal_raw_opinion_clusters_observations --> courtlistener_raw_opinion_clusters
       ala_publicportal_raw_opinion_clusters_observations --> courtlistener_raw_opinions
       ala_publicportal_raw_opinion_clusters_observations --> courtlistener_staged_opinion_clusters
       ala_publicportal_raw_opinion_clusters_observations --> courtlistener_staged_opinions
       ala_publicportal_raw_oral_arguments --> ala_publicportal_latest_oral_arguments
       ala_publicportal_raw_oral_arguments --> ala_publicportal_staged_oral_arguments
       ala_publicportal_raw_oral_arguments --> courtlistener_raw_audio
       ala_publicportal_raw_oral_arguments --> courtlistener_staged_audio
       ala_publicportal_raw_oral_arguments_observations --> ala_publicportal_latest_oral_arguments
       ala_publicportal_raw_oral_arguments_observations --> ala_publicportal_staged_oral_arguments
       ala_publicportal_raw_oral_arguments_observations --> courtlistener_raw_audio
       ala_publicportal_raw_oral_arguments_observations --> courtlistener_staged_audio
       ala_publicportal_staged_docket_entries --> courtlistener_raw_docket_entries
       ala_publicportal_staged_docket_entries --> courtlistener_staged_docket_entries
       ala_publicportal_staged_dockets --> ala_publicportal_irr_invalid_court_ids_dockets
       ala_publicportal_staged_dockets --> courtlistener_irr_invalid_court_ids_dockets
       ala_publicportal_staged_dockets --> courtlistener_irr_null_docket_numbers_dockets
       ala_publicportal_staged_dockets --> courtlistener_raw_dockets
       ala_publicportal_staged_dockets --> courtlistener_raw_originating_court_information
       ala_publicportal_staged_dockets --> courtlistener_staged_dockets
       ala_publicportal_staged_dockets --> courtlistener_staged_originating_court_information
       ala_publicportal_staged_opinion_clusters --> ala_publicportal_irr_future_dates_opinion_clusters
       ala_publicportal_staged_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       ala_publicportal_staged_opinion_clusters --> courtlistener_raw_opinion_clusters
       ala_publicportal_staged_opinion_clusters --> courtlistener_staged_opinion_clusters
       ala_publicportal_staged_opinions --> courtlistener_raw_opinions
       ala_publicportal_staged_opinions --> courtlistener_staged_opinions
       ala_publicportal_staged_oral_arguments --> courtlistener_raw_audio
       ala_publicportal_staged_oral_arguments --> courtlistener_staged_audio
       conn_jud_ct_gov_corrections_docket_entries --> conn_jud_ct_gov_staged_docket_entries
       conn_jud_ct_gov_corrections_docket_entries --> courtlistener_raw_docket_entries
       conn_jud_ct_gov_corrections_docket_entries --> courtlistener_staged_docket_entries
       conn_jud_ct_gov_corrections_dockets --> conn_jud_ct_gov_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_corrections_dockets --> conn_jud_ct_gov_staged_dockets
       conn_jud_ct_gov_corrections_dockets --> courtlistener_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_corrections_dockets --> courtlistener_irr_null_docket_numbers_dockets
       conn_jud_ct_gov_corrections_dockets --> courtlistener_raw_dockets
       conn_jud_ct_gov_corrections_dockets --> courtlistener_raw_originating_court_information
       conn_jud_ct_gov_corrections_dockets --> courtlistener_staged_dockets
       conn_jud_ct_gov_corrections_dockets --> courtlistener_staged_originating_court_information
       conn_jud_ct_gov_corrections_opinion_clusters --> conn_jud_ct_gov_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_corrections_opinion_clusters --> conn_jud_ct_gov_staged_opinion_clusters
       conn_jud_ct_gov_corrections_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_corrections_opinion_clusters --> courtlistener_raw_opinion_clusters
       conn_jud_ct_gov_corrections_opinion_clusters --> courtlistener_staged_opinion_clusters
       conn_jud_ct_gov_corrections_oral_arguments --> conn_jud_ct_gov_staged_oral_arguments
       conn_jud_ct_gov_corrections_oral_arguments --> courtlistener_raw_audio
       conn_jud_ct_gov_corrections_oral_arguments --> courtlistener_staged_audio
       conn_jud_ct_gov_latest_docket_entries --> conn_jud_ct_gov_staged_docket_entries
       conn_jud_ct_gov_latest_docket_entries --> courtlistener_raw_docket_entries
       conn_jud_ct_gov_latest_docket_entries --> courtlistener_staged_docket_entries
       conn_jud_ct_gov_latest_dockets --> conn_jud_ct_gov_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_latest_dockets --> conn_jud_ct_gov_staged_docket_parties
       conn_jud_ct_gov_latest_dockets --> conn_jud_ct_gov_staged_dockets
       conn_jud_ct_gov_latest_dockets --> courtlistener_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_latest_dockets --> courtlistener_irr_null_docket_numbers_dockets
       conn_jud_ct_gov_latest_dockets --> courtlistener_raw_dockets
       conn_jud_ct_gov_latest_dockets --> courtlistener_raw_originating_court_information
       conn_jud_ct_gov_latest_dockets --> courtlistener_staged_dockets
       conn_jud_ct_gov_latest_dockets --> courtlistener_staged_originating_court_information
       conn_jud_ct_gov_latest_opinion_clusters --> conn_jud_ct_gov_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_latest_opinion_clusters --> conn_jud_ct_gov_staged_opinion_clusters
       conn_jud_ct_gov_latest_opinion_clusters --> conn_jud_ct_gov_staged_opinions
       conn_jud_ct_gov_latest_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_latest_opinion_clusters --> courtlistener_raw_opinion_clusters
       conn_jud_ct_gov_latest_opinion_clusters --> courtlistener_raw_opinions
       conn_jud_ct_gov_latest_opinion_clusters --> courtlistener_staged_opinion_clusters
       conn_jud_ct_gov_latest_opinion_clusters --> courtlistener_staged_opinions
       conn_jud_ct_gov_latest_oral_arguments --> conn_jud_ct_gov_staged_oral_arguments
       conn_jud_ct_gov_latest_oral_arguments --> courtlistener_raw_audio
       conn_jud_ct_gov_latest_oral_arguments --> courtlistener_staged_audio
       conn_jud_ct_gov_raw_docket_entries --> conn_jud_ct_gov_latest_docket_entries
       conn_jud_ct_gov_raw_docket_entries --> conn_jud_ct_gov_staged_docket_entries
       conn_jud_ct_gov_raw_docket_entries --> courtlistener_raw_docket_entries
       conn_jud_ct_gov_raw_docket_entries --> courtlistener_staged_docket_entries
       conn_jud_ct_gov_raw_docket_entries_observations --> conn_jud_ct_gov_latest_docket_entries
       conn_jud_ct_gov_raw_docket_entries_observations --> conn_jud_ct_gov_staged_docket_entries
       conn_jud_ct_gov_raw_docket_entries_observations --> courtlistener_raw_docket_entries
       conn_jud_ct_gov_raw_docket_entries_observations --> courtlistener_staged_docket_entries
       conn_jud_ct_gov_raw_dockets --> conn_jud_ct_gov_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_raw_dockets --> conn_jud_ct_gov_latest_dockets
       conn_jud_ct_gov_raw_dockets --> conn_jud_ct_gov_staged_docket_parties
       conn_jud_ct_gov_raw_dockets --> conn_jud_ct_gov_staged_dockets
       conn_jud_ct_gov_raw_dockets --> courtlistener_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_raw_dockets --> courtlistener_irr_null_docket_numbers_dockets
       conn_jud_ct_gov_raw_dockets --> courtlistener_raw_dockets
       conn_jud_ct_gov_raw_dockets --> courtlistener_raw_originating_court_information
       conn_jud_ct_gov_raw_dockets --> courtlistener_staged_dockets
       conn_jud_ct_gov_raw_dockets --> courtlistener_staged_originating_court_information
       conn_jud_ct_gov_raw_dockets_observations --> conn_jud_ct_gov_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_raw_dockets_observations --> conn_jud_ct_gov_latest_dockets
       conn_jud_ct_gov_raw_dockets_observations --> conn_jud_ct_gov_staged_docket_parties
       conn_jud_ct_gov_raw_dockets_observations --> conn_jud_ct_gov_staged_dockets
       conn_jud_ct_gov_raw_dockets_observations --> courtlistener_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_raw_dockets_observations --> courtlistener_irr_null_docket_numbers_dockets
       conn_jud_ct_gov_raw_dockets_observations --> courtlistener_raw_dockets
       conn_jud_ct_gov_raw_dockets_observations --> courtlistener_raw_originating_court_information
       conn_jud_ct_gov_raw_dockets_observations --> courtlistener_staged_dockets
       conn_jud_ct_gov_raw_dockets_observations --> courtlistener_staged_originating_court_information
       conn_jud_ct_gov_raw_opinion_clusters --> conn_jud_ct_gov_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters --> conn_jud_ct_gov_latest_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters --> conn_jud_ct_gov_staged_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters --> conn_jud_ct_gov_staged_opinions
       conn_jud_ct_gov_raw_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters --> courtlistener_raw_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters --> courtlistener_raw_opinions
       conn_jud_ct_gov_raw_opinion_clusters --> courtlistener_staged_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters --> courtlistener_staged_opinions
       conn_jud_ct_gov_raw_opinion_clusters_observations --> conn_jud_ct_gov_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters_observations --> conn_jud_ct_gov_latest_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters_observations --> conn_jud_ct_gov_staged_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters_observations --> conn_jud_ct_gov_staged_opinions
       conn_jud_ct_gov_raw_opinion_clusters_observations --> courtlistener_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters_observations --> courtlistener_raw_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters_observations --> courtlistener_raw_opinions
       conn_jud_ct_gov_raw_opinion_clusters_observations --> courtlistener_staged_opinion_clusters
       conn_jud_ct_gov_raw_opinion_clusters_observations --> courtlistener_staged_opinions
       conn_jud_ct_gov_raw_oral_arguments --> conn_jud_ct_gov_latest_oral_arguments
       conn_jud_ct_gov_raw_oral_arguments --> conn_jud_ct_gov_staged_oral_arguments
       conn_jud_ct_gov_raw_oral_arguments --> courtlistener_raw_audio
       conn_jud_ct_gov_raw_oral_arguments --> courtlistener_staged_audio
       conn_jud_ct_gov_raw_oral_arguments_observations --> conn_jud_ct_gov_latest_oral_arguments
       conn_jud_ct_gov_raw_oral_arguments_observations --> conn_jud_ct_gov_staged_oral_arguments
       conn_jud_ct_gov_raw_oral_arguments_observations --> courtlistener_raw_audio
       conn_jud_ct_gov_raw_oral_arguments_observations --> courtlistener_staged_audio
       conn_jud_ct_gov_staged_docket_entries --> courtlistener_raw_docket_entries
       conn_jud_ct_gov_staged_docket_entries --> courtlistener_staged_docket_entries
       conn_jud_ct_gov_staged_dockets --> conn_jud_ct_gov_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_staged_dockets --> courtlistener_irr_invalid_court_ids_dockets
       conn_jud_ct_gov_staged_dockets --> courtlistener_irr_null_docket_numbers_dockets
       conn_jud_ct_gov_staged_dockets --> courtlistener_raw_dockets
       conn_jud_ct_gov_staged_dockets --> courtlistener_raw_originating_court_information
       conn_jud_ct_gov_staged_dockets --> courtlistener_staged_dockets
       conn_jud_ct_gov_staged_dockets --> courtlistener_staged_originating_court_information
       conn_jud_ct_gov_staged_opinion_clusters --> conn_jud_ct_gov_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_staged_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       conn_jud_ct_gov_staged_opinion_clusters --> courtlistener_raw_opinion_clusters
       conn_jud_ct_gov_staged_opinion_clusters --> courtlistener_staged_opinion_clusters
       conn_jud_ct_gov_staged_opinions --> courtlistener_raw_opinions
       conn_jud_ct_gov_staged_opinions --> courtlistener_staged_opinions
       conn_jud_ct_gov_staged_oral_arguments --> courtlistener_raw_audio
       conn_jud_ct_gov_staged_oral_arguments --> courtlistener_staged_audio
       courtlistener_corrections_audio --> courtlistener_staged_audio
       courtlistener_corrections_docket_entries --> courtlistener_staged_docket_entries
       courtlistener_corrections_dockets --> courtlistener_irr_invalid_court_ids_dockets
       courtlistener_corrections_dockets --> courtlistener_irr_null_docket_numbers_dockets
       courtlistener_corrections_dockets --> courtlistener_staged_dockets
       courtlistener_corrections_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       courtlistener_corrections_opinion_clusters --> courtlistener_staged_opinion_clusters
       courtlistener_corrections_opinions --> courtlistener_staged_opinions
       courtlistener_corrections_originating_court_information --> courtlistener_staged_originating_court_information
       courtlistener_raw_audio --> courtlistener_staged_audio
       courtlistener_raw_docket_entries --> courtlistener_staged_docket_entries
       courtlistener_raw_dockets --> courtlistener_irr_invalid_court_ids_dockets
       courtlistener_raw_dockets --> courtlistener_irr_null_docket_numbers_dockets
       courtlistener_raw_dockets --> courtlistener_staged_dockets
       courtlistener_raw_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       courtlistener_raw_opinion_clusters --> courtlistener_staged_opinion_clusters
       courtlistener_raw_opinions --> courtlistener_staged_opinions
       courtlistener_raw_originating_court_information --> courtlistener_staged_originating_court_information
       courtlistener_staged_dockets --> courtlistener_irr_invalid_court_ids_dockets
       courtlistener_staged_dockets --> courtlistener_irr_null_docket_numbers_dockets
       courtlistener_staged_opinion_clusters --> courtlistener_irr_future_dates_opinion_clusters
       warehouse_court_ids --> ala_publicportal_irr_future_dates_opinion_clusters
       warehouse_court_ids --> ala_publicportal_irr_invalid_court_ids_dockets
       warehouse_court_ids --> ala_publicportal_staged_dockets
       warehouse_court_ids --> ala_publicportal_staged_opinion_clusters
       warehouse_court_ids --> conn_jud_ct_gov_irr_future_dates_opinion_clusters
       warehouse_court_ids --> conn_jud_ct_gov_irr_invalid_court_ids_dockets
       warehouse_court_ids --> conn_jud_ct_gov_staged_dockets
       warehouse_court_ids --> conn_jud_ct_gov_staged_opinion_clusters
       warehouse_court_ids --> courtlistener_irr_future_dates_opinion_clusters
       warehouse_court_ids --> courtlistener_irr_invalid_court_ids_dockets
       warehouse_court_ids --> courtlistener_irr_null_docket_numbers_dockets
       warehouse_court_ids --> courtlistener_raw_dockets
       warehouse_court_ids --> courtlistener_raw_opinion_clusters
       warehouse_court_ids --> courtlistener_raw_originating_court_information
       warehouse_court_ids --> courtlistener_staged_dockets
       warehouse_court_ids --> courtlistener_staged_opinion_clusters
       warehouse_court_ids --> courtlistener_staged_originating_court_information
