# NBA Analytics

This project analyzes historical NBA team and player performance with a local DuckDB warehouse built from Kaggle data.

## Scope

- team-level differences between wins and losses
- recent team ranking since 2024, with a focused Thunder profile
- historical player benchmarks by role
- recent active player tiers and rankings against those historical standards

## Data

Source dataset:

- Kaggle: `eoinamoore/historical-nba-data-and-player-box-scores`

This project uses a local DuckDB database. The source CSV files can be downloaded with `kagglehub` or placed manually in `data/`.

## Local Build

A successful local build produced:

- `1,666,760` rows in `player_stats_fact`
- `146,318` rows in `team_stats_fact`
- `6,691` players
- `51` team dimension rows
- `73,350` games

Validation details are in [`docs/VALIDATION_RESULTS.md`](docs/VALIDATION_RESULTS.md).

## Method

- Historical benchmark pool: player-seasons with at least `750` minutes and `30` games
- Role assignment: inferred `Creator`, `Wing`, and `Big` roles from box-score production
- Benchmark stats: `Points / 36`, `Assists / 36`, `3P Made / 36`, `Steals / 36`, `Rebounds / 36`, and `Blocks / 36`
- Tier cutoffs: `50th`, `75th`, and `95th` percentiles within each role
- Recent active-player pool: players who appeared in calendar years `2024` or `2025`, using each player's latest qualified season from the last three NBA seasons
- Current-player qualification: latest qualified season with at least `1000` minutes and `30` games

## Visuals

### Winning Drivers

![Winning Drivers](images/winning-drivers.png)

Compares team-level counting stats and shooting percentages in wins and losses.

### Thunder Profile

![Thunder Profile](images/thunder-profile.png)

Ranks recent teams by win rate since 2024 and compares Thunder's winning profile to historical win and loss averages.

### Historical Role Benchmarks

![Historical Role Benchmarks](images/historical-role-benchmarks.png)

Shows the `50th`, `75th`, and `95th` percentile benchmarks for key player stats within each role.

### Recent Active Player Tiers

![Recent Active Player Tiers](images/active-role-tiers.png)

Shows the distribution of recent active players across `Low`, `50`, `75`, and `95` overall tiers by role.

## Files

```text
NBA-Analytics/
README.md
requirements.txt
.gitignore
notebooks/
scripts/
sql/
docs/
data/
images/
```

## Setup

### 1. Create a Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Build the local DuckDB database

If the NBA CSV files are not already in `data/`, download them and build the database:

```bash
python scripts/build_local_duckdb.py --download --force
```

If the CSV files already exist in `data/`, build the database directly:

```bash
python scripts/build_local_duckdb.py --force
```

This creates `data/nba_analytics.duckdb`.

### 3. Run the notebooks

- [`notebooks/01_build_database.ipynb`](notebooks/01_build_database.ipynb)
- [`notebooks/02_validation.ipynb`](notebooks/02_validation.ipynb)
- [`notebooks/03_analysis.ipynb`](notebooks/03_analysis.ipynb)

### 4. Generate the visual files

```bash
python scripts/generate_visuals.py
```

## Notes

- The current workflow uses DuckDB only.
- Team-level results are more stable than player-position results built from raw source flags.
- The player benchmark is based on box-score production, not possession-level impact measures.
- Historical team naming variation and some unresolved fact links still exist in the local build.
