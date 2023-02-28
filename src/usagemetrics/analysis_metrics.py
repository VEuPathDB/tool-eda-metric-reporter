from dataclasses import dataclass

import pandas as pd


@dataclass
class AnalysisMetrics:
    """
    Data class containing analysis metrics results.

    user_stats_histogram:
    E.G.
    objects_bucket    registered_users_with_analysis_count   guests_users_with_filter_count...
    0                                       0.0                              0.0...
    1                                       7.0                            980.0...
    2                                       2.0                            191.0...
    <=4                                     8.0                             89.0...
    <=8                                     3.0                             45.0...
    <=16                                    1.0                             19.0...
    <=32                                    1.0                              6.0...
    <=64                                    1.0                              0.0...
    >64                                     2.0                              0.0...

    study_stats:
      Columns: Study ID, Study Name, Number of analyses, Number of shares, Curated or User Study

    registered_totals_stats/guest_totals_stats:
        dict containing 'numUsers','numAnalyses','numFilters','numVisualizations' counts
    """
    user_stats_histogram: pd.DataFrame
    study_stats: pd.DataFrame
    registered_totals_stats: dict
    guest_totals_stats: dict
    raw_output: dict
