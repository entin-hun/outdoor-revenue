# Budakeszi Hourly Attendance Heuristic

Python tool to estimate hourly outdoor-event attendance in Budakeszi (HU) using historical weather data and calendar effects.

## Features

- Open-Meteo archive API data download (`temperature_2m`, `precipitation`, `surface_pressure`)
- Hungarian holiday handling via `holidays.country_holidays('HU')`
- Opening-hours aware attendance model
- Per-hour multipliers: `M_calendar`, `M_temp`, `M_rain`, `M_front`
- Final estimate: `estimated_attendance = 40 * M_calendar * M_temp * M_rain * M_front`
- Revenue estimate driven by an integer unit input (default: 2000 HUF)
- Hourly gross baseline at 100% ratio is unit input * 100 (default: 200000 HUF)
- Unified dashboard HTML: weekly line chart by year and yearly summary table in one view

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Example

Generate full-year 2024 output:

```bash
python attendance_heuristic.py \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --unit-revenue-huf 2000 \
  --ratio-percent 100 \
  --csv-output output/attendance_2024.csv \
  --plot-output output/dashboard_2024.html
```

Generate full-year 2023 output:

```bash
python attendance_heuristic.py \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --unit-revenue-huf 2000 \
  --ratio-percent 100 \
  --csv-output output/attendance_2023.csv \
  --plot-output output/dashboard_2023.html
```

## Output Columns

- `timestamp`
- `temperature_2m`
- `precipitation`
- `surface_pressure`
- `M_calendar`
- `M_temp`
- `M_rain`
- `M_front`
- `estimated_attendance`
- `capacity_ratio`
- `estimated_hourly_gross_revenue_huf`

Open the generated `*.html` file in a browser for the interactive chart.

## GitHub Pages

Generate directly to a Pages-compatible path:

```bash
python attendance_heuristic.py \
  --start-date 2024-03-01 \
  --end-date 2025-12-31 \
  --unit-revenue-huf 2000 \
  --ratio-percent 100 \
  --csv-output docs/attendance_2024_2025.csv \
  --plot-output docs/index.html
```

Then push `docs/index.html` and enable GitHub Pages from the `docs` folder in repository settings.
