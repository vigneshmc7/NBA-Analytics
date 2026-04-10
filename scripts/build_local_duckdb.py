#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb
import pandas as pd

try:
    import kagglehub
except ImportError:  # pragma: no cover - optional until download is requested
    kagglehub = None


DATASET_ID = "eoinamoore/historical-nba-data-and-player-box-scores"
REQUIRED_FILES = ("TeamStatistics.csv", "PlayerStatistics.csv", "Players.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local DuckDB database for the NBA Analytics project."
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing the NBA CSV files.",
    )
    parser.add_argument(
        "--db-path",
        default="data/nba_analytics.duckdb",
        help="Path to the DuckDB database file to create.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the Kaggle dataset into the data directory before building.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing DuckDB database file.",
    )
    return parser.parse_args()


def ensure_data_files(data_dir: Path, download: bool) -> dict[str, Path]:
    data_dir.mkdir(parents=True, exist_ok=True)

    if download:
        if kagglehub is None:
            raise RuntimeError(
                "kagglehub is not installed. Install requirements first or provide the CSV files manually."
            )

        dataset_path = Path(kagglehub.dataset_download(DATASET_ID))
        for file_name in REQUIRED_FILES:
            src = dataset_path / file_name
            dst = data_dir / file_name
            shutil.copy2(src, dst)

    files = {file_name: data_dir / file_name for file_name in REQUIRED_FILES}
    missing = [path.name for path in files.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required data files: "
            + ", ".join(missing)
            + ". Use --download or place the CSVs in the data directory."
        )
    return files


def read_csv(path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        na_values=["", "None", "NULL"],
        keep_default_na=True,
        low_memory=False,
    )
    df.columns = [str(col).strip() for col in df.columns]

    if parse_dates:
        for col in parse_dates:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def normalize_num_minutes(series: pd.Series) -> pd.Series:
    def parse_value(value: object) -> float | None:
        if pd.isna(value):
            return None

        text = str(value).strip()
        if not text:
            return None

        if ":" in text:
            minutes, seconds = text.split(":", 1)
            try:
                return float(minutes) + (float(seconds) / 60.0)
            except ValueError:
                return None

        try:
            return float(text)
        except ValueError:
            return None

    return series.apply(parse_value)


def clean_players(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy().rename(
        columns={
            "birthDate": "birthdate",
            "school": "lastAttended",
            "heightInches": "height",
            "bodyWeightLbs": "bodyWeight",
        }
    )

    flag_map = {
        "True": 1,
        "False": 0,
        "true": 1,
        "false": 0,
        "1": 1,
        "0": 0,
        1: 1,
        0: 0,
        True: 1,
        False: 0,
    }

    for col in ("guard", "forward", "center"):
        if col in cleaned.columns:
            cleaned[col] = cleaned[col].map(flag_map)
            cleaned[col] = cleaned[col].astype("Int64")

    for col in ("draftYear", "draftRound", "draftNumber"):
        if col in cleaned.columns:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
            cleaned[col] = cleaned[col].mask(cleaned[col].isin([-1, -22]))

    if "height" in cleaned.columns:
        cleaned["height"] = pd.to_numeric(cleaned["height"], errors="coerce")
        cleaned["height"] = cleaned["height"].mask(
            (cleaned["height"] < 65) | (cleaned["height"] > 90)
        )

    if "bodyWeight" in cleaned.columns:
        cleaned["bodyWeight"] = pd.to_numeric(cleaned["bodyWeight"], errors="coerce")
        cleaned["bodyWeight"] = cleaned["bodyWeight"].mask(
            (cleaned["bodyWeight"] < 120) | (cleaned["bodyWeight"] > 400)
        )

    if "birthdate" in cleaned.columns:
        cleaned["birthdate"] = pd.to_datetime(cleaned["birthdate"], errors="coerce")

    return cleaned


def clean_team_stats(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    if "numMinutes" in cleaned.columns:
        cleaned["numMinutes"] = normalize_num_minutes(cleaned["numMinutes"])
    return cleaned


def clean_player_stats(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    if "numMinutes" in cleaned.columns:
        cleaned["numMinutes"] = normalize_num_minutes(cleaned["numMinutes"])
    return cleaned


def create_raw_tables(con: duckdb.DuckDBPyConnection, team_df: pd.DataFrame, player_stats_df: pd.DataFrame, players_df: pd.DataFrame) -> None:
    con.register("team_stats_src", team_df)
    con.register("player_stats_src", player_stats_df)
    con.register("players_src", players_df)

    con.execute("CREATE OR REPLACE TABLE team_stats_raw AS SELECT * FROM team_stats_src")
    con.execute("CREATE OR REPLACE TABLE player_stats_raw AS SELECT * FROM player_stats_src")
    con.execute("CREATE OR REPLACE TABLE players_raw AS SELECT * FROM players_src")

    con.unregister("team_stats_src")
    con.unregister("player_stats_src")
    con.unregister("players_src")


def build_star_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE players AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY personId) AS playerKey,
            personId,
            firstName,
            lastName,
            CAST(birthdate AS DATE) AS birthdate,
            lastAttended,
            country,
            CAST(height AS DOUBLE) AS height,
            CAST(bodyWeight AS DOUBLE) AS bodyWeight,
            CAST(guard AS INTEGER) AS guard,
            CAST(forward AS INTEGER) AS forward,
            CAST(center AS INTEGER) AS center,
            CAST(draftYear AS DOUBLE) AS draftYear,
            CAST(draftRound AS DOUBLE) AS draftRound,
            CAST(draftNumber AS DOUBLE) AS draftNumber
        FROM players_raw
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE teams AS
        WITH team_history AS (
            SELECT
                teamName,
                teamCity,
                MAX(CAST(coachId AS DOUBLE)) AS coachId,
                MAX(CAST(gameDateTimeEst AS TIMESTAMP)) AS last_seen
            FROM team_stats_raw
            GROUP BY teamName, teamCity
        ),
        ranked_teams AS (
            SELECT
                teamName,
                teamCity,
                coachId,
                ROW_NUMBER() OVER (
                    PARTITION BY teamName
                    ORDER BY
                        last_seen DESC,
                        CASE WHEN teamCity IS NULL THEN 1 ELSE 0 END,
                        teamCity DESC
                ) AS team_row_rank
            FROM team_history
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY teamName) AS teamKey,
            teamName,
            teamCity,
            coachId
        FROM ranked_teams
        WHERE team_row_rank = 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE team_name_lookup AS
        SELECT
            teamName,
            MIN(teamKey) AS teamKey
        FROM teams
        GROUP BY teamName
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE games AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY gameId) AS gameKey,
            gameId,
            COALESCE(MAX(gameType), 'Unknown') AS gameType
        FROM (
            SELECT
                CAST(gameId AS BIGINT) AS gameId,
                CASE
                    WHEN gameType IS NULL OR TRIM(gameType) = '' THEN NULL
                    WHEN LOWER(REPLACE(TRIM(gameType), ' ', '')) = 'preseason' THEN 'Preseason'
                    WHEN LOWER(TRIM(gameType)) = 'regular season' THEN 'Regular Season'
                    WHEN LOWER(TRIM(gameType)) IN ('emirates nba cup', 'in-season-knockout') THEN 'In-Season Tournament'
                    ELSE TRIM(gameType)
                END AS gameType
            FROM player_stats_raw
        ) src
        GROUP BY gameId
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dates AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY gameDateEst) AS dateKey,
            gameDateEst,
            STRFTIME(gameDateEst, '%Y-%m-%d') AS fullDate,
            CAST(EXTRACT(YEAR FROM gameDateEst) AS INTEGER) AS year,
            CAST(EXTRACT(MONTH FROM gameDateEst) AS INTEGER) AS month,
            CAST(EXTRACT(DAY FROM gameDateEst) AS INTEGER) AS day,
            CAST(EXTRACT(DAYOFWEEK FROM gameDateEst) AS INTEGER) AS dayOfTheWeek,
            CAST(EXTRACT(QUARTER FROM gameDateEst) AS INTEGER) AS quarter
        FROM (
            SELECT DISTINCT CAST(gameDateTimeEst AS TIMESTAMP) AS gameDateEst
            FROM team_stats_raw
            UNION
            SELECT DISTINCT CAST(gameDateTimeEst AS TIMESTAMP) AS gameDateEst
            FROM player_stats_raw
        ) src
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE player_stats_fact AS
        SELECT
            p.playerKey,
            team_lookup.teamKey AS teamKey,
            opp_lookup.teamKey AS opponentTeamKey,
            g.gameKey,
            d.dateKey,
            ps.gameType,
            ps.gameLabel,
            ps.gameSubLabel,
            ps.seriesGameNumber,
            CAST(ps.win AS INTEGER) AS win,
            CAST(ps.home AS INTEGER) AS home,
            CAST(ps.numMinutes AS DOUBLE) AS numMinutes,
            CAST(ps.points AS DOUBLE) AS points,
            CAST(ps.assists AS DOUBLE) AS assists,
            CAST(ps.blocks AS DOUBLE) AS blocks,
            CAST(ps.steals AS DOUBLE) AS steals,
            CAST(ps.fieldGoalsAttempted AS DOUBLE) AS fieldGoalsAttempted,
            CAST(ps.fieldGoalsMade AS DOUBLE) AS fieldGoalsMade,
            CAST(ps.fieldGoalsPercentage AS DOUBLE) AS fieldGoalsPercentage,
            CAST(ps.threePointersAttempted AS DOUBLE) AS threePointersAttempted,
            CAST(ps.threePointersMade AS DOUBLE) AS threePointersMade,
            CAST(ps.threePointersPercentage AS DOUBLE) AS threePointersPercentage,
            CAST(ps.freeThrowsAttempted AS DOUBLE) AS freeThrowsAttempted,
            CAST(ps.freeThrowsMade AS DOUBLE) AS freeThrowsMade,
            CAST(ps.freeThrowsPercentage AS DOUBLE) AS freeThrowsPercentage,
            CAST(ps.reboundsDefensive AS DOUBLE) AS reboundsDefensive,
            CAST(ps.reboundsOffensive AS DOUBLE) AS reboundsOffensive,
            CAST(ps.reboundsTotal AS DOUBLE) AS reboundsTotal,
            CAST(ps.foulsPersonal AS DOUBLE) AS foulsPersonal,
            CAST(ps.turnovers AS DOUBLE) AS turnovers,
            CAST(ps.plusMinusPoints AS DOUBLE) AS plusMinusPoints
        FROM player_stats_raw ps
        LEFT JOIN players p
            ON ps.personId = p.personId
        LEFT JOIN team_name_lookup team_lookup
            ON ps.playerteamName = team_lookup.teamName
        LEFT JOIN team_name_lookup opp_lookup
            ON ps.opponentteamName = opp_lookup.teamName
        LEFT JOIN games g
            ON ps.gameId = g.gameId
        LEFT JOIN dates d
            ON CAST(ps.gameDateTimeEst AS TIMESTAMP) = d.gameDateEst
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE team_stats_fact AS
        SELECT
            team_lookup.teamKey AS teamKey,
            opp_lookup.teamKey AS opponentTeamKey,
            g.gameKey,
            d.dateKey,
            CAST(ts.home AS INTEGER) AS home,
            CAST(ts.win AS INTEGER) AS win,
            CAST(ts.teamScore AS DOUBLE) AS teamScore,
            CAST(ts.opponentScore AS DOUBLE) AS opponentScore,
            CAST(ts.assists AS DOUBLE) AS assists,
            CAST(ts.blocks AS DOUBLE) AS blocks,
            CAST(ts.steals AS DOUBLE) AS steals,
            CAST(ts.fieldGoalsAttempted AS DOUBLE) AS fieldGoalsAttempted,
            CAST(ts.fieldGoalsMade AS DOUBLE) AS fieldGoalsMade,
            CAST(ts.fieldGoalsPercentage AS DOUBLE) AS fieldGoalsPercentage,
            CAST(ts.threePointersAttempted AS DOUBLE) AS threePointersAttempted,
            CAST(ts.threePointersMade AS DOUBLE) AS threePointersMade,
            CAST(ts.threePointersPercentage AS DOUBLE) AS threePointersPercentage,
            CAST(ts.freeThrowsAttempted AS DOUBLE) AS freeThrowsAttempted,
            CAST(ts.freeThrowsMade AS DOUBLE) AS freeThrowsMade,
            CAST(ts.freeThrowsPercentage AS DOUBLE) AS freeThrowsPercentage,
            CAST(ts.reboundsDefensive AS DOUBLE) AS reboundsDefensive,
            CAST(ts.reboundsOffensive AS DOUBLE) AS reboundsOffensive,
            CAST(ts.reboundsTotal AS DOUBLE) AS reboundsTotal,
            CAST(ts.foulsPersonal AS DOUBLE) AS foulsPersonal,
            CAST(ts.turnovers AS DOUBLE) AS turnovers,
            CAST(ts.plusMinusPoints AS DOUBLE) AS plusMinusPoints,
            CAST(ts.numMinutes AS DOUBLE) AS numMinutes,
            CAST(ts.q1Points AS DOUBLE) AS q1Points,
            CAST(ts.q2Points AS DOUBLE) AS q2Points,
            CAST(ts.q3Points AS DOUBLE) AS q3Points,
            CAST(ts.q4Points AS DOUBLE) AS q4Points,
            CAST(ts.benchPoints AS DOUBLE) AS benchPoints,
            CAST(ts.biggestLead AS DOUBLE) AS biggestLead,
            CAST(ts.biggestScoringRun AS DOUBLE) AS biggestScoringRun,
            CAST(ts.leadChanges AS DOUBLE) AS leadChanges,
            CAST(ts.pointsFastBreak AS DOUBLE) AS pointsFastBreak,
            CAST(ts.pointsFromTurnovers AS DOUBLE) AS pointsFromTurnovers,
            CAST(ts.pointsInThePaint AS DOUBLE) AS pointsInThePaint,
            CAST(ts.pointsSecondChance AS DOUBLE) AS pointsSecondChance,
            CAST(ts.timesTied AS DOUBLE) AS timesTied,
            CAST(ts.timeoutsRemaining AS DOUBLE) AS timeoutsRemaining,
            CAST(ts.seasonWins AS DOUBLE) AS seasonWins,
            CAST(ts.seasonLosses AS DOUBLE) AS seasonLosses
        FROM team_stats_raw ts
        LEFT JOIN team_name_lookup team_lookup
            ON ts.teamName = team_lookup.teamName
        LEFT JOIN team_name_lookup opp_lookup
            ON ts.opponentTeamName = opp_lookup.teamName
        LEFT JOIN games g
            ON ts.gameId = g.gameId
        LEFT JOIN dates d
            ON CAST(ts.gameDateTimeEst AS TIMESTAMP) = d.gameDateEst
        """
    )
    con.execute("DROP TABLE IF EXISTS team_name_lookup")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    summary = con.execute(
        """
        SELECT * FROM (
            SELECT 'team_stats_raw' AS table_name, COUNT(*) AS row_count FROM team_stats_raw
            UNION ALL
            SELECT 'player_stats_raw', COUNT(*) FROM player_stats_raw
            UNION ALL
            SELECT 'players_raw', COUNT(*) FROM players_raw
            UNION ALL
            SELECT 'players', COUNT(*) FROM players
            UNION ALL
            SELECT 'teams', COUNT(*) FROM teams
            UNION ALL
            SELECT 'games', COUNT(*) FROM games
            UNION ALL
            SELECT 'dates', COUNT(*) FROM dates
            UNION ALL
            SELECT 'player_stats_fact', COUNT(*) FROM player_stats_fact
            UNION ALL
            SELECT 'team_stats_fact', COUNT(*) FROM team_stats_fact
        )
        ORDER BY table_name
        """
    ).fetchdf()
    print(summary.to_string(index=False))


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)

    files = ensure_data_files(data_dir=data_dir, download=args.download)

    if db_path.exists():
        if not args.force:
            raise FileExistsError(
                f"{db_path} already exists. Use --force to rebuild it."
            )
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)

    team_df = clean_team_stats(
        read_csv(files["TeamStatistics.csv"], parse_dates=["gameDateTimeEst"])
    )
    player_stats_df = clean_player_stats(
        read_csv(files["PlayerStatistics.csv"], parse_dates=["gameDateTimeEst"])
    )
    players_df = clean_players(read_csv(files["Players.csv"], parse_dates=["birthdate"]))

    con = duckdb.connect(str(db_path))
    try:
        create_raw_tables(con, team_df, player_stats_df, players_df)
        build_star_schema(con)
        print(f"Built local DuckDB database at {db_path}")
        print_summary(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
