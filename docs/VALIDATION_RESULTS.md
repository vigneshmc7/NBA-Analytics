# Validation Results

Results captured from the local DuckDB build created with:

```bash
python scripts/build_local_duckdb.py --force
```

Database file:

- `data/nba_analytics.duckdb`

## Warehouse Build Summary

The local build completed successfully and created these row counts:

| Table | Rows |
| --- | ---: |
| `player_stats_raw` | 1,666,760 |
| `player_stats_fact` | 1,666,760 |
| `team_stats_raw` | 146,318 |
| `team_stats_fact` | 146,318 |
| `players` | 6,691 |
| `teams` | 51 |
| `games` | 73,350 |
| `dates` | 34,886 |

## Validation Findings

Read-only checks from `sql/validation_checks_duckdb.sql` show:

- `players.personId` is unique in the local players dimension.
- `games.gameId` is unique in the local games dimension.
- The team dimension keeps one row per `teamName`, using the most recent city label found in the source data.
- `player_stats_fact` still has unresolved links:
  - 234 missing `playerKey`
  - 5,123 missing `teamKey`
  - 5,123 missing `opponentTeamKey`
  - 3,728 missing `dateKey`
- `team_stats_fact` still has a small number of unresolved links:
  - 2 missing `teamKey`
  - 2 missing `opponentTeamKey`
  - 4 missing `gameKey`
- 381 players carry more than one position flag.
- 37 active players in 2024/2025 carry more than one position flag.

## Analytical Checks

### Top real teams since 2024

Using only teams with a non-null `teamCity` and at least 100 games since 2024:

| Team | City | Win % Since 2024 | Games |
| --- | --- | ---: | ---: |
| Thunder | Oklahoma City | 76.26 | 257 |
| Celtics | Boston | 72.51 | 251 |
| Cavaliers | Cleveland | 64.73 | 241 |
| Knicks | New York | 63.78 | 254 |
| Nuggets | Denver | 62.60 | 246 |
| Rockets | Houston | 60.53 | 228 |
| Lakers | Los Angeles | 58.87 | 231 |
| Timberwolves | Minnesota | 58.82 | 255 |
| Clippers | LA | 57.26 | 234 |
| Warriors | Golden State | 55.27 | 237 |

Thunder is the highest-win-rate real team in the dataset since 2024 among teams with substantial sample size.

### Win vs loss profile

Across team-level game records, wins are associated with:

- more assists: 24.95 vs 21.49
- more field goals made: 41.53 vs 37.60
- more 3-pointers made: 6.75 vs 5.74
- more defensive rebounds: 32.88 vs 29.08
- fewer fouls: 21.98 vs 23.23
- fewer turnovers: 14.39 vs 15.35

These results support the team-level analysis.

## Interpretation

- team-level findings are reasonably supported
- Thunder is a valid recent reference team after excluding tiny-sample pseudo-teams
- raw Guard/Forward/Center player tiers should be treated as provisional until position overlap and unresolved links are cleaned up
- the role-based historical player-season benchmark is more stable than the raw source-position approach
