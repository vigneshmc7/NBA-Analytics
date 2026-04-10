#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DB_PATH = Path("data/nba_analytics.duckdb")
OUT_DIR = Path("images")

BG = "#f7f4ea"
INK = "#14213d"
MUTED = "#5a6472"
GRID = "#d8d2c4"
GOLD = "#d6a54b"
GREEN = "#3f7d5d"
RED = "#b84a3a"
BLUE = "#3975b7"
TEAL = "#2a7f9e"
SLATE = "#748091"


def style_matplotlib() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": BG,
            "axes.facecolor": BG,
            "axes.edgecolor": BG,
            "axes.labelcolor": INK,
            "xtick.color": INK,
            "ytick.color": INK,
            "text.color": INK,
            "font.size": 11,
            "axes.titlesize": 16,
            "axes.titleweight": "bold",
            "axes.labelsize": 11,
            "figure.titlesize": 20,
            "legend.frameon": False,
        }
    )


def ensure_output_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def get_connection() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"{DB_PATH} does not exist. Run scripts/build_local_duckdb.py first."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


def fetch_win_loss_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            win,
            AVG(assists) AS assists,
            AVG(blocks) AS blocks,
            AVG(steals) AS steals,
            AVG(fieldGoalsMade) AS fieldGoalsMade,
            AVG(threePointersMade) AS threePointersMade,
            AVG(freeThrowsMade) AS freeThrowsMade,
            AVG(reboundsDefensive) AS reboundsDefensive,
            AVG(reboundsOffensive) AS reboundsOffensive,
            AVG(benchPoints) AS benchPoints,
            AVG(fieldGoalsPercentage) * 100 AS fieldGoalsPercentage,
            AVG(threePointersPercentage) * 100 AS threePointersPercentage,
            AVG(freeThrowsPercentage) * 100 AS freeThrowsPercentage,
            AVG(foulsPersonal) AS foulsPersonal,
            AVG(turnovers) AS turnovers
        FROM team_stats_fact
        WHERE win IN (0, 1)
        GROUP BY win
        ORDER BY win
        """
    ).fetchdf()


def fetch_top_teams(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            t.teamName,
            t.teamCity,
            ROUND(AVG(CASE WHEN tf.win = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_pct_2024_plus,
            COUNT(*) AS games_count
        FROM team_stats_fact tf
        JOIN teams t ON t.teamKey = tf.teamKey
        JOIN dates d ON d.dateKey = tf.dateKey
        WHERE d.year >= 2024
          AND t.teamCity IS NOT NULL
        GROUP BY t.teamName, t.teamCity
        HAVING COUNT(*) >= 100
        ORDER BY win_pct_2024_plus DESC, games_count DESC
        LIMIT 10
        """
    ).fetchdf()


def fetch_thunder_profile(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            AVG(tf.assists) AS assists,
            AVG(tf.blocks) AS blocks,
            AVG(tf.steals) AS steals,
            AVG(tf.fieldGoalsMade) AS fieldGoalsMade,
            AVG(tf.threePointersMade) AS threePointersMade,
            AVG(tf.freeThrowsMade) AS freeThrowsMade,
            AVG(tf.reboundsDefensive) AS reboundsDefensive,
            AVG(tf.reboundsOffensive) AS reboundsOffensive,
            AVG(tf.benchPoints) AS benchPoints,
            AVG(tf.foulsPersonal) AS foulsPersonal,
            AVG(tf.turnovers) AS turnovers
        FROM team_stats_fact tf
        JOIN teams t ON t.teamKey = tf.teamKey
        JOIN dates d ON d.dateKey = tf.dateKey
        WHERE d.year >= 2024
          AND LOWER(t.teamName) = 'thunder'
          AND tf.win = 1
        """
    ).fetchdf()

def fetch_player_season_pool(
    con: duckdb.DuckDBPyConnection,
    min_minutes: int = 750,
    min_games: int = 30,
) -> pd.DataFrame:
    df = con.execute(
        f"""
        WITH player_seasons AS (
            SELECT
                CASE WHEN d.month >= 10 THEN d.year + 1 ELSE d.year END AS season_end_year,
                p.playerKey,
                p.firstName,
                p.lastName,
                COUNT(DISTINCT ps.gameKey) AS games_played,
                SUM(ps.numMinutes) AS total_minutes,
                SUM(ps.points) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS pts36,
                SUM(ps.assists) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS ast36,
                SUM(ps.steals) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS stl36,
                SUM(ps.blocks) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS blk36,
                SUM(ps.threePointersMade) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS tpm36,
                SUM(ps.threePointersAttempted) AS three_pa_total,
                SUM(ps.threePointersAttempted) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS tpa36,
                SUM(ps.reboundsDefensive) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS dreb36,
                SUM(ps.reboundsOffensive) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS oreb36,
                SUM(ps.turnovers) / NULLIF(SUM(ps.numMinutes), 0) * 36 AS tov36,
                SUM(ps.fieldGoalsAttempted) AS field_goal_attempts,
                SUM(ps.fieldGoalsMade) / NULLIF(SUM(ps.fieldGoalsAttempted), 0) AS fg_pct,
                SUM(ps.threePointersMade) / NULLIF(SUM(ps.threePointersAttempted), 0) AS tp_pct
            FROM player_stats_fact ps
            JOIN players p ON p.playerKey = ps.playerKey
            JOIN dates d ON d.dateKey = ps.dateKey
            GROUP BY 1, 2, 3, 4
            HAVING SUM(ps.numMinutes) >= {min_minutes}
               AND COUNT(DISTINCT ps.gameKey) >= {min_games}
        )
        SELECT * FROM player_seasons
        """
    ).fetchdf()

    if df.empty:
        raise ValueError("No player seasons met the benchmark thresholds.")

    df["player"] = df["firstName"] + " " + df["lastName"]
    df["season_label"] = (
        (df["season_end_year"] - 1).astype(int).astype(str)
        + "-"
        + df["season_end_year"].astype(int).astype(str).str[-2:]
    )

    # Use attempt thresholds so low-volume shooting seasons do not distort percentile bands.
    df["tp_pct"] = df["tp_pct"].where(df["three_pa_total"] >= 125)
    df["fg_pct"] = df["fg_pct"].where(df["field_goal_attempts"] >= 250)
    df["tp_pct"] = df["tp_pct"].fillna(df["tp_pct"].median())
    df["fg_pct"] = df["fg_pct"].fillna(df["fg_pct"].median())
    return df


def _percentile_rank(series: pd.Series, ascending: bool = True) -> pd.Series:
    return series.rank(pct=True, method="average", ascending=ascending) * 100


def build_role_benchmark_frame(player_seasons: pd.DataFrame) -> pd.DataFrame:
    df = player_seasons.copy()
    metric_cols = [
        "pts36",
        "ast36",
        "stl36",
        "blk36",
        "tpm36",
        "dreb36",
        "oreb36",
        "tov36",
        "fg_pct",
        "tp_pct",
    ]

    zscores = (
        df[metric_cols] - df[metric_cols].mean()
    ) / df[metric_cols].std(ddof=0).replace(0, 1)

    df["creator_signal"] = (
        1.00 * zscores["ast36"]
        + 0.45 * zscores["pts36"]
        + 0.30 * zscores["tpm36"]
        - 0.15 * zscores["oreb36"]
        - 0.10 * zscores["blk36"]
    )
    df["wing_signal"] = (
        0.70 * zscores["pts36"]
        + 0.55 * zscores["tpm36"]
        + 0.30 * zscores["tp_pct"]
        + 0.35 * zscores["stl36"]
        + 0.15 * zscores["dreb36"]
    )
    df["big_signal"] = (
        0.40 * zscores["pts36"]
        + 0.60 * zscores["dreb36"]
        + 0.70 * zscores["oreb36"]
        + 0.70 * zscores["blk36"]
        + 0.35 * zscores["fg_pct"]
        - 0.35 * zscores["tpm36"]
    )

    role_map = {
        "creator_signal": "Creator",
        "wing_signal": "Wing",
        "big_signal": "Big",
    }
    df["role"] = (
        df[["creator_signal", "wing_signal", "big_signal"]]
        .idxmax(axis=1)
        .map(role_map)
    )

    # Reclassify strong interior seasons that look like bigs despite mild wing signals.
    interior_mask = (
        (df["role"] == "Wing")
        & ((df["dreb36"] + df["oreb36"] >= 11) | (df["blk36"] >= 1.5))
        & (df["tpm36"] < 1.8)
    )
    df.loc[interior_mask, "role"] = "Big"

    scored_frames: list[pd.DataFrame] = []
    for role, role_df in df.groupby("role", sort=False):
        temp = role_df.copy()
        temp["pillar_scoring"] = _percentile_rank(temp["pts36"])
        temp["pillar_playmaking"] = (
            0.75 * _percentile_rank(temp["ast36"])
            + 0.25 * _percentile_rank(temp["tov36"], ascending=False)
        )
        temp["pillar_shooting"] = (
            0.65 * _percentile_rank(temp["tpm36"])
            + 0.35 * _percentile_rank(temp["tp_pct"])
        )
        temp["pillar_defense"] = (
            0.55 * _percentile_rank(temp["stl36"])
            + 0.45 * _percentile_rank(temp["blk36"])
        )
        temp["pillar_rebounding"] = (
            0.70 * _percentile_rank(temp["dreb36"])
            + 0.30 * _percentile_rank(temp["oreb36"])
        )

        if role == "Creator":
            weights = {
                "pillar_scoring": 0.25,
                "pillar_playmaking": 0.35,
                "pillar_shooting": 0.20,
                "pillar_defense": 0.10,
                "pillar_rebounding": 0.10,
            }
        elif role == "Wing":
            weights = {
                "pillar_scoring": 0.30,
                "pillar_playmaking": 0.10,
                "pillar_shooting": 0.25,
                "pillar_defense": 0.20,
                "pillar_rebounding": 0.15,
            }
        else:
            weights = {
                "pillar_scoring": 0.20,
                "pillar_playmaking": 0.05,
                "pillar_shooting": 0.05,
                "pillar_defense": 0.35,
                "pillar_rebounding": 0.35,
            }

        temp["overall_score"] = sum(temp[col] * weight for col, weight in weights.items())
        temp["historical_percentile_within_role"] = _percentile_rank(temp["overall_score"])
        temp["historical_rank_within_role"] = temp["overall_score"].rank(
            ascending=False,
            method="dense",
        )
        temp["historical_pool_size"] = len(temp)
        scored_frames.append(temp)

    return pd.concat(scored_frames, ignore_index=True)

def fetch_historical_role_benchmarks(
    con: duckdb.DuckDBPyConnection,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    historical = build_role_benchmark_frame(fetch_player_season_pool(con))
    historical["reb36"] = historical["dreb36"] + historical["oreb36"]

    metric_meta = [
        ("pts36", "Points / 36"),
        ("ast36", "Assists / 36"),
        ("tpm36", "3P Made / 36"),
        ("stl36", "Steals / 36"),
        ("reb36", "Rebounds / 36"),
        ("blk36", "Blocks / 36"),
    ]
    role_order = ["Creator", "Wing", "Big"]
    rows = []
    for role in role_order:
        role_df = historical[historical["role"] == role]
        for metric, label in metric_meta:
            vals = role_df[metric].dropna()
            rows.append(
                {
                    "role": role,
                    "metric": metric,
                    "label": label,
                    "p50": float(vals.quantile(0.50)),
                    "p75": float(vals.quantile(0.75)),
                    "p95": float(vals.quantile(0.95)),
                    "pool_size": int(len(vals)),
                }
            )
    return historical, pd.DataFrame(rows)


def fetch_current_player_tiers(
    con: duckdb.DuckDBPyConnection,
    active_min_minutes: int = 1000,
    active_min_games: int = 30,
    active_years: tuple[int, ...] = (2024, 2025),
    recent_seasons: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    historical, benchmark_df = fetch_historical_role_benchmarks(con)
    latest_season = int(historical["season_end_year"].max())
    season_window = list(range(latest_season - recent_seasons + 1, latest_season + 1))
    active_year_sql = ", ".join(str(year) for year in active_years)
    active_ids = set(
        con.execute(
            f"""
            SELECT DISTINCT ps.playerKey
            FROM player_stats_fact ps
            JOIN dates d ON d.dateKey = ps.dateKey
            WHERE d.year IN ({active_year_sql})
              AND ps.playerKey IS NOT NULL
            """
        ).fetchdf()["playerKey"]
    )
    active = historical[
        historical["playerKey"].isin(active_ids)
        & historical["season_end_year"].isin(season_window)
        & (historical["total_minutes"] >= active_min_minutes)
        & (historical["games_played"] >= active_min_games)
    ].copy()
    active = (
        active.sort_values(
            ["playerKey", "season_end_year", "total_minutes", "games_played"],
            ascending=[True, True, False, False],
        )
        .groupby("playerKey", as_index=False)
        .tail(1)
        .copy()
    )

    if active.empty:
        raise ValueError("No recent active players met the current-pool thresholds.")
    active["current_rank_overall"] = (
        active["overall_score"].rank(ascending=False, method="dense").astype(int)
    )
    active["current_rank_within_role"] = (
        active.groupby("role")["overall_score"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    threshold_map: dict[str, dict[str, dict[int, float]]] = {}
    for row in benchmark_df.itertuples():
        threshold_map.setdefault(row.role, {})[row.metric] = {
            50: row.p50,
            75: row.p75,
            95: row.p95,
        }

    role_keys = {
        "Creator": ["pts36", "ast36", "tpm36"],
        "Wing": ["pts36", "tpm36", "stl36"],
        "Big": ["pts36", "reb36", "blk36"],
    }
    metrics = ["pts36", "ast36", "tpm36", "stl36", "reb36", "blk36"]

    def classify_metric(value: float, cuts: dict[int, float]) -> object:
        if pd.isna(value):
            return "Low"
        if value >= cuts[95]:
            return 95
        if value >= cuts[75]:
            return 75
        if value >= cuts[50]:
            return 50
        return "Low"

    active["reb36"] = active["dreb36"] + active["oreb36"]
    for metric in metrics:
        active[f"{metric}_tier"] = active.apply(
            lambda row: classify_metric(row[metric], threshold_map[row["role"]][metric]),
            axis=1,
        )

    def assign_overall_tier(row: pd.Series) -> object:
        role = row["role"]
        key_metrics = role_keys[role]
        other_metrics = [metric for metric in metrics if metric not in key_metrics]

        metric_tiers = {metric: row[f"{metric}_tier"] for metric in metrics}
        key95 = sum(metric_tiers[m] == 95 for m in key_metrics)
        key75p = sum(metric_tiers[m] in [75, 95] for m in key_metrics)
        key50p = sum(metric_tiers[m] in [50, 75, 95] for m in key_metrics)
        other75p = sum(metric_tiers[m] in [75, 95] for m in other_metrics)

        if (key95 >= 2) or ((key95 == 1) and (other75p >= 2)):
            return 95
        if (key75p >= 2) or ((key75p >= 1) and (other75p >= 2)):
            return 75
        if key50p >= 1:
            return 50
        return "Low"

    active["overall_tier"] = active.apply(assign_overall_tier, axis=1)

    counts = (
        active.groupby(["role", "overall_tier"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=["Creator", "Wing", "Big"], columns=["Low", 50, 75, 95], fill_value=0)
    )
    start_end_year = min(season_window)
    window_label = f"{start_end_year - 1}-{str(start_end_year)[-2:]} to {latest_season - 1}-{str(latest_season)[-2:]}"
    return historical, active, window_label


def plot_winning_drivers(win_loss: pd.DataFrame) -> Path:
    df = win_loss.copy()
    df["result"] = df["win"].map({1: "Win", 0: "Loss"})
    count_cols = [
        "assists",
        "blocks",
        "steals",
        "fieldGoalsMade",
        "threePointersMade",
        "freeThrowsMade",
        "reboundsDefensive",
        "reboundsOffensive",
        "foulsPersonal",
        "turnovers",
        "benchPoints",
    ]
    pct_cols = [
        "fieldGoalsPercentage",
        "threePointersPercentage",
        "freeThrowsPercentage",
    ]

    count_labels = {
        "assists": "Assists",
        "blocks": "Blocks",
        "steals": "Steals",
        "fieldGoalsMade": "FG Made",
        "threePointersMade": "3P Made",
        "freeThrowsMade": "FT Made",
        "reboundsDefensive": "Def. Rebounds",
        "reboundsOffensive": "Off. Rebounds",
        "foulsPersonal": "Fouls",
        "turnovers": "Turnovers",
        "benchPoints": "Bench Points",
    }
    counts = (
        df.set_index("result")[count_cols]
        .T[["Win", "Loss"]]
        .rename_axis("metric")
        .reset_index()
    )
    counts["label"] = counts["metric"].map(count_labels)
    counts["gap"] = counts["Win"] - counts["Loss"]
    counts = counts.sort_values("gap", ascending=True).reset_index(drop=True)

    pct = df.set_index("result")[pct_cols].T[["Win", "Loss"]]
    pct.index = ["FG%", "3PT%", "FT%"]

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(12, 10),
        gridspec_kw={"height_ratios": [2.1, 1]},
    )

    colors = {"Win": GREEN, "Loss": RED}
    y = np.arange(len(counts))
    height = 0.38

    for idx, result in enumerate(["Win", "Loss"]):
        offset = (idx - 0.5) * height
        bars = ax1.barh(
            y + offset,
            counts[result],
            height=height,
            color=colors[result],
            edgecolor=BG,
            linewidth=1.0,
            label=result,
        )
        for bar in bars:
            ax1.text(
                bar.get_width() + 0.35,
                bar.get_y() + bar.get_height() / 2,
                f"{bar.get_width():.1f}",
                va="center",
                ha="left",
                fontsize=9,
                color=MUTED,
            )

    ax1.set_yticks(y)
    ax1.set_yticklabels(counts["label"])
    ax1.set_xlabel("Average Per Game")
    ax1.set_title("Winning Drivers: Counting Stats In Wins vs Losses")
    ax1.legend(loc="lower right")
    ax1.grid(axis="x", color=GRID, linestyle="--", linewidth=0.8, alpha=0.9)
    ax1.set_axisbelow(True)

    x = np.arange(len(pct.index))
    width = 0.34
    for idx, result in enumerate(["Win", "Loss"]):
        bars = ax2.bar(
            x + (idx - 0.5) * width,
            pct[result],
            width=width,
            color=colors[result],
            edgecolor=BG,
            linewidth=1.0,
            label=result,
        )
        for bar in bars:
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.9,
                f"{bar.get_height():.1f}",
                va="bottom",
                ha="center",
                fontsize=9,
                color=MUTED,
            )

    ax2.set_xticks(x)
    ax2.set_xticklabels(pct.index)
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("Average Shooting %")
    ax2.set_title("Winning Drivers: Shooting Efficiency In Wins vs Losses")
    ax2.legend(loc="upper left")
    ax2.grid(axis="y", color=GRID, linestyle="--", linewidth=0.8, alpha=0.9)
    ax2.set_axisbelow(True)

    for ax in (ax1, ax2):
        for spine in ax.spines.values():
            spine.set_visible(False)

    fig.suptitle("Winning Drivers", y=0.98, fontsize=19, fontweight="bold")
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    out_path = OUT_DIR / "winning-drivers.png"
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    return out_path


def plot_thunder_profile(
    top_teams: pd.DataFrame,
    win_loss: pd.DataFrame,
    thunder_profile: pd.DataFrame,
) -> Path:
    compare_cols = [
        "assists",
        "blocks",
        "steals",
        "fieldGoalsMade",
        "threePointersMade",
        "freeThrowsMade",
        "reboundsDefensive",
        "reboundsOffensive",
        "foulsPersonal",
        "turnovers",
        "benchPoints",
    ]
    compare_labels = [
        "Assists",
        "Blocks",
        "Steals",
        "FG Made",
        "3P Made",
        "FT Made",
        "Def. Rebounds",
        "Off. Rebounds",
        "Fouls",
        "Turnovers",
        "Bench Points",
    ]
    win_row = win_loss.loc[win_loss["win"] == 1].iloc[0]
    loss_row = win_loss.loc[win_loss["win"] == 0].iloc[0]
    thunder = thunder_profile.iloc[0]
    compare_df = pd.DataFrame(
        {
            "stat": compare_labels,
            "Winning Avg (All Years)": [float(win_row[col]) for col in compare_cols],
            "Losing Avg (All Years)": [float(loss_row[col]) for col in compare_cols],
            "Thunder 2024+ Wins": [float(thunder[col]) for col in compare_cols],
        }
    )

    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(12, 10),
        gridspec_kw={"height_ratios": [1, 2]},
    )

    ranked = top_teams.sort_values("win_pct_2024_plus", ascending=False).copy()
    ranked["label"] = ranked["teamCity"] + " " + ranked["teamName"]
    colors = [GOLD if name == "Thunder" else SLATE for name in ranked["teamName"]]
    y = np.arange(len(ranked))
    ax1.barh(y, ranked["win_pct_2024_plus"], color=colors, height=0.7)
    ax1.set_yticks(y)
    ax1.set_yticklabels(ranked["label"])
    ax1.invert_yaxis()
    ax1.tick_params(axis="y", labelsize=10)
    ax1.set_xlabel("Win % Since 2024")
    ax1.set_title("Recent Team Ranking")
    ax1.grid(axis="x", color=GRID, linestyle="--", linewidth=0.8, alpha=0.9)
    ax1.set_axisbelow(True)
    for idx, value in enumerate(ranked["win_pct_2024_plus"]):
        ax1.text(value + 0.6, idx, f"{value:.1f}", va="center", ha="left", fontsize=10, color=MUTED)

    y2 = np.arange(len(compare_df))
    height = 0.24
    groups = ["Winning Avg (All Years)", "Losing Avg (All Years)", "Thunder 2024+ Wins"]
    group_colors = {
        "Winning Avg (All Years)": GREEN,
        "Losing Avg (All Years)": RED,
        "Thunder 2024+ Wins": BLUE,
    }
    for idx, col in enumerate(groups):
        offset = (idx - 1) * height
        bars = ax2.barh(
            y2 + offset,
            compare_df[col],
            height=height,
            label=col,
            color=group_colors[col],
            edgecolor=BG,
            linewidth=1.0,
        )
        for bar in bars:
            ax2.text(
                bar.get_width() + 0.25,
                bar.get_y() + bar.get_height() / 2,
                f"{bar.get_width():.1f}",
                va="center",
                ha="left",
                fontsize=8,
                color=MUTED,
            )

    ax2.set_yticks(y2)
    ax2.set_yticklabels(compare_df["stat"])
    ax2.set_xlabel("Average Per Game")
    ax2.set_title("Thunder Profile vs Historical Win/Loss Benchmarks")
    ax2.grid(axis="x", color=GRID, linestyle="--", linewidth=0.8, alpha=0.9)
    ax2.set_axisbelow(True)
    ax2.legend(loc="lower right")

    fig.suptitle("Recent Team Ranking And Thunder Profile", y=0.98, fontsize=19, fontweight="bold")
    for ax in (ax1, ax2):
        for spine in ax.spines.values():
            spine.set_visible(False)

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    out_path = OUT_DIR / "thunder-profile.png"
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    return out_path


def plot_historical_role_benchmarks(benchmark_df: pd.DataFrame) -> Path:
    roles = ["Creator", "Wing", "Big"]
    metric_order = [
        "Points / 36",
        "Assists / 36",
        "3P Made / 36",
        "Steals / 36",
        "Rebounds / 36",
        "Blocks / 36",
    ]
    tier_colors = {50: SLATE, 75: GOLD, 95: BLUE}
    x = np.arange(len(roles))
    bar_width = 0.22

    fig, axes = plt.subplots(3, 2, figsize=(13, 10))
    axes = axes.flatten()

    for ax, label in zip(axes, metric_order):
        metric_df = benchmark_df[benchmark_df["label"] == label].copy()
        metric_df["role"] = pd.Categorical(metric_df["role"], categories=roles, ordered=True)
        metric_df = metric_df.sort_values("role")

        vals_50 = metric_df["p50"].tolist()
        vals_75 = metric_df["p75"].tolist()
        vals_95 = metric_df["p95"].tolist()

        bars_50 = ax.bar(x - bar_width, vals_50, width=bar_width, color=tier_colors[50], label="50th percentile")
        bars_75 = ax.bar(x, vals_75, width=bar_width, color=tier_colors[75], label="75th percentile")
        bars_95 = ax.bar(x + bar_width, vals_95, width=bar_width, color=tier_colors[95], label="95th percentile")

        for bars in [bars_50, bars_75, bars_95]:
            for bar in bars:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(0.05, bar.get_height() * 0.02),
                    f"{bar.get_height():.1f}",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    color=INK,
                )

        ax.set_title(label)
        ax.set_xticks(x)
        ax.set_xticklabels(roles)
        ax.grid(axis="y", color=GRID, linestyle="--", linewidth=0.8, alpha=0.9)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)

    handles = [
        plt.Line2D([0], [0], color=tier_colors[p], lw=10, label=f"{p}th percentile")
        for p in [50, 75, 95]
    ]
    fig.legend(handles=handles, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 0.92))
    fig.suptitle(
        "Historical Role Benchmarks",
        y=0.98,
        fontsize=18,
        fontweight="bold",
    )
    fig.tight_layout(rect=[0, 0.04, 1, 0.93])
    out_path = OUT_DIR / "historical-role-benchmarks.png"
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    return out_path


def plot_current_player_tiers(active_players: pd.DataFrame, window_label: str) -> Path:
    counts = (
        active_players.groupby(["role", "overall_tier"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=["Creator", "Wing", "Big"], columns=["Low", 50, 75, 95], fill_value=0)
    )
    x = np.arange(len(counts.index))
    tier_order = ["Low", 50, 75, 95]
    colors = {"Low": "#c4c4c4", 50: TEAL, 75: GOLD, 95: BLUE}
    fig, ax = plt.subplots(figsize=(9.5, 6.8))
    bottom = np.zeros(len(counts.index))

    for tier in tier_order:
        vals = counts[tier].to_numpy()
        bars = ax.bar(
            x,
            vals,
            bottom=bottom,
            color=colors[tier],
            edgecolor=BG,
            linewidth=1.2,
            label=str(tier),
            width=0.62,
        )
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_y() + bar.get_height() / 2,
                    str(int(val)),
                    ha="center",
                    va="center",
                    fontsize=10,
                    color=INK,
                    fontweight="bold",
                )
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(counts.index)
    ax.set_ylabel("# Recent Active Players")
    ax.set_xlabel("Role")
    ax.set_title(f"Recent Active Player Tiers By Role ({window_label})")
    ax.grid(axis="y", color=GRID, linestyle="--", linewidth=0.8, alpha=0.9)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.legend(title="Overall Tier", loc="upper right")

    for idx, total in enumerate(counts.sum(axis=1).to_numpy()):
        ax.text(x[idx], total + 1.5, f"Total {int(total)}", ha="center", va="bottom", fontsize=10, color=MUTED)

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    out_path = OUT_DIR / "active-role-tiers.png"
    fig.savefig(out_path, dpi=240, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    return out_path


def main() -> None:
    style_matplotlib()
    ensure_output_dir()
    con = get_connection()

    try:
        win_loss = fetch_win_loss_summary(con)
        top_teams = fetch_top_teams(con)
        thunder_profile = fetch_thunder_profile(con)
        _, historical_benchmarks = fetch_historical_role_benchmarks(con)
        _, active_players, active_window_label = fetch_current_player_tiers(con)

        outputs = [
            plot_winning_drivers(win_loss),
            plot_thunder_profile(top_teams, win_loss, thunder_profile),
            plot_historical_role_benchmarks(historical_benchmarks),
            plot_current_player_tiers(active_players, active_window_label),
        ]
    finally:
        con.close()

    print("Generated files:")
    for path in outputs:
        print(f"- {path}")


if __name__ == "__main__":
    main()
