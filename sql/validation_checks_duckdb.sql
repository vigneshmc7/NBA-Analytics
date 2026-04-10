-- Validation queries for the local DuckDB build.
-- These checks assume scripts/build_local_duckdb.py has already created the database.

-- 1. Confirm expected tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
  AND table_name IN (
      'team_stats_raw',
      'player_stats_raw',
      'players_raw',
      'players',
      'teams',
      'games',
      'dates',
      'player_stats_fact',
      'team_stats_fact'
  )
ORDER BY table_name;

-- 2. Duplicate personId check in players dimension
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT personId) AS distinct_person_ids,
    COUNT(*) - COUNT(DISTINCT personId) AS duplicate_person_id_rows
FROM players;

-- 3. Duplicate gameId check in games dimension
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT gameId) AS distinct_game_ids,
    COUNT(*) - COUNT(DISTINCT gameId) AS duplicate_game_id_rows
FROM games;

-- 4. Team dimension ambiguity by name
SELECT
    teamName,
    COUNT(*) AS dimension_rows,
    COUNT(DISTINCT teamCity) AS city_count,
    COUNT(DISTINCT coachId) AS coach_count
FROM teams
GROUP BY teamName
HAVING COUNT(*) > 1
ORDER BY dimension_rows DESC, teamName;

-- 5. Unresolved foreign-key-style fields in player fact table
SELECT
    SUM(CASE WHEN playerKey IS NULL THEN 1 ELSE 0 END) AS missing_playerkey,
    SUM(CASE WHEN teamKey IS NULL THEN 1 ELSE 0 END) AS missing_teamkey,
    SUM(CASE WHEN opponentTeamKey IS NULL THEN 1 ELSE 0 END) AS missing_opponentteamkey,
    SUM(CASE WHEN gameKey IS NULL THEN 1 ELSE 0 END) AS missing_gamekey,
    SUM(CASE WHEN dateKey IS NULL THEN 1 ELSE 0 END) AS missing_datekey
FROM player_stats_fact;

-- 6. Unresolved foreign-key-style fields in team fact table
SELECT
    SUM(CASE WHEN teamKey IS NULL THEN 1 ELSE 0 END) AS missing_teamkey,
    SUM(CASE WHEN opponentTeamKey IS NULL THEN 1 ELSE 0 END) AS missing_opponentteamkey,
    SUM(CASE WHEN gameKey IS NULL THEN 1 ELSE 0 END) AS missing_gamekey,
    SUM(CASE WHEN dateKey IS NULL THEN 1 ELSE 0 END) AS missing_datekey
FROM team_stats_fact;

-- 7. Position-flag distribution in players dimension
SELECT
    guard,
    forward,
    center,
    COUNT(*) AS players
FROM players
GROUP BY guard, forward, center
ORDER BY players DESC, guard, forward, center;

-- 8. Players carrying multiple position flags
SELECT
    COUNT(*) AS multi_position_players
FROM players
WHERE COALESCE(guard, 0) + COALESCE(forward, 0) + COALESCE(center, 0) > 1;

-- 9. Active players in 2024/2025 who carry multiple position flags
SELECT
    COUNT(DISTINCT p.playerKey) AS active_multi_position_players
FROM players p
JOIN player_stats_fact ps ON ps.playerKey = p.playerKey
JOIN dates d ON d.dateKey = ps.dateKey
WHERE d.year IN (2024, 2025)
  AND COALESCE(p.guard, 0) + COALESCE(p.forward, 0) + COALESCE(p.center, 0) > 1;

-- 10. Thunder lookup by team name
SELECT DISTINCT
    teamKey,
    teamName,
    teamCity
FROM teams
WHERE LOWER(teamName) = 'thunder'
ORDER BY teamKey;

-- 11. Top teams since 2024 by win percentage
SELECT
    tf.teamKey,
    t.teamName,
    t.teamCity,
    ROUND(AVG(CASE WHEN tf.win = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_pct_2024_plus,
    COUNT(*) AS games_count
FROM team_stats_fact tf
JOIN teams t ON t.teamKey = tf.teamKey
JOIN dates d ON d.dateKey = tf.dateKey
WHERE d.year >= 2024
GROUP BY tf.teamKey, t.teamName, t.teamCity
ORDER BY win_pct_2024_plus DESC, games_count DESC
LIMIT 10;

-- 12. Thunder profile since 2024, resolved by name
SELECT
    t.teamName,
    t.teamCity,
    ROUND(AVG(tf.assists), 2) AS assists,
    ROUND(AVG(tf.blocks), 2) AS blocks,
    ROUND(AVG(tf.steals), 2) AS steals,
    ROUND(AVG(tf.fieldGoalsMade), 2) AS fieldGoalsMade,
    ROUND(AVG(tf.threePointersMade), 2) AS threePointersMade,
    ROUND(AVG(tf.freeThrowsMade), 2) AS freeThrowsMade,
    ROUND(AVG(tf.reboundsDefensive), 2) AS reboundsDefensive,
    ROUND(AVG(tf.reboundsOffensive), 2) AS reboundsOffensive,
    ROUND(AVG(tf.foulsPersonal), 2) AS foulsPersonal,
    ROUND(AVG(tf.turnovers), 2) AS turnovers,
    ROUND(AVG(tf.benchPoints), 2) AS benchPoints
FROM team_stats_fact tf
JOIN teams t ON t.teamKey = tf.teamKey
JOIN dates d ON d.dateKey = tf.dateKey
WHERE d.year >= 2024
  AND LOWER(t.teamName) = 'thunder'
  AND tf.win = 1
GROUP BY t.teamName, t.teamCity;
