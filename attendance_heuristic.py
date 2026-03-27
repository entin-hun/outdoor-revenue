#!/usr/bin/env python3
"""Heuristic hourly attendance estimation for Budakeszi outdoor events.

Data source: Open-Meteo archive API.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import holidays
import pandas as pd
import plotly.graph_objects as go
import requests

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 47.4991121
LONGITUDE = 18.9176924
MAX_CAPACITY = 40


@dataclass(frozen=True)
class AttendanceConfig:
    latitude: float = LATITUDE
    longitude: float = LONGITUDE
    max_capacity: int = MAX_CAPACITY


def fetch_historical_weather(
    start_date: str,
    end_date: str,
    *,
    latitude: float = LATITUDE,
    longitude: float = LONGITUDE,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch hourly historical weather data from Open-Meteo archive API."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,surface_pressure",
        "timezone": "Europe/Budapest",
    }

    response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    hourly = payload.get("hourly")
    if not hourly:
        raise ValueError("Open-Meteo response missing 'hourly' data.")

    required_keys = ["time", "temperature_2m", "precipitation", "surface_pressure"]
    missing = [k for k in required_keys if k not in hourly]
    if missing:
        raise ValueError(f"Open-Meteo response missing required hourly keys: {missing}")

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(hourly["time"]),
            "temperature_2m": hourly["temperature_2m"],
            "precipitation": hourly["precipitation"],
            "surface_pressure": hourly["surface_pressure"],
        }
    )
    return df


def _is_leisure_day(ts: pd.Timestamp, hu_holidays: holidays.HolidayBase) -> bool:
    return ts.weekday() >= 5 or ts.date() in hu_holidays


def _in_opening_hours(ts: pd.Timestamp, leisure_day: bool) -> bool:
    # Closing hour is exclusive: e.g. 12:00-19:59 on weekdays.
    start_hour = 10 if leisure_day else 12
    end_hour = 18 if leisure_day else 20
    return start_hour <= ts.hour < end_hour


def calendar_multiplier(ts: pd.Timestamp, hu_holidays: holidays.HolidayBase) -> float:
    leisure = _is_leisure_day(ts, hu_holidays)
    if not _in_opening_hours(ts, leisure):
        return 0.0
    if leisure:
        return 1.0
    # Weekday ramp-up: 0.2 until 16:00, then 0.6 until closing.
    return 0.6 if ts.hour >= 16 else 0.2


def temperature_multiplier(temperature_2m: float) -> float:
    if pd.isna(temperature_2m):
        return 0.0
    if temperature_2m < 16.0 or temperature_2m > 30.0:
        return 0.0
    if 21.0 <= temperature_2m <= 25.0:
        return 1.0
    if 16.0 <= temperature_2m < 21.0:
        # Linear ramp-up from 0 at 16C to 1 at 21C.
        return (temperature_2m - 16.0) / 5.0
    # Linear ramp-down from 1 at 25C to 0 at 30C.
    return (30.0 - temperature_2m) / 5.0


def rain_multiplier(precipitation: float) -> float:
    if pd.isna(precipitation):
        return 0.1
    if precipitation == 0:
        return 1.0
    if 0 < precipitation <= 1.0:
        return 0.5
    return 0.1


def front_multiplier(surface_pressure: float, pressure_3h_ago: float) -> float:
    if pd.isna(surface_pressure) or pd.isna(pressure_3h_ago):
        return 1.0
    delta_p = surface_pressure - pressure_3h_ago
    return 0.75 if abs(delta_p) >= 3.0 else 1.0


def estimate_hourly_attendance(
    weather_df: pd.DataFrame,
    *,
    max_capacity: int = MAX_CAPACITY,
    hu_holidays: holidays.HolidayBase | None = None,
) -> pd.DataFrame:
    """Estimate attendance per hour with heuristic multipliers."""
    if weather_df.empty:
        return weather_df.copy()

    if hu_holidays is None:
        years = sorted({ts.year for ts in weather_df["timestamp"] if pd.notna(ts)})
        hu_holidays = holidays.country_holidays("HU", years=years)

    df = weather_df.copy().sort_values("timestamp").reset_index(drop=True)

    df["M_calendar"] = df["timestamp"].apply(lambda ts: calendar_multiplier(ts, hu_holidays))
    df["M_temp"] = df["temperature_2m"].apply(temperature_multiplier)
    df["M_rain"] = df["precipitation"].apply(rain_multiplier)
    df["surface_pressure_3h_ago"] = df["surface_pressure"].shift(3)
    df["M_front"] = df.apply(
        lambda row: front_multiplier(row["surface_pressure"], row["surface_pressure_3h_ago"]),
        axis=1,
    )

    raw_estimate = (
        max_capacity * df["M_calendar"] * df["M_temp"] * df["M_rain"] * df["M_front"]
    )
    df["estimated_attendance"] = raw_estimate.round().astype(int)

    # Enforce zero attendance outside opening hours (already implied by M_calendar=0, kept explicit).
    df.loc[df["M_calendar"] == 0.0, "estimated_attendance"] = 0

    out_cols = [
        "timestamp",
        "temperature_2m",
        "precipitation",
        "surface_pressure",
        "M_calendar",
        "M_temp",
        "M_rain",
        "M_front",
        "estimated_attendance",
    ]
    return df[out_cols]


def build_attendance_dataset(
    start_date: str,
    end_date: str,
    config: AttendanceConfig | None = None,
) -> pd.DataFrame:
    cfg = config or AttendanceConfig()
    weather = fetch_historical_weather(
        start_date=start_date,
        end_date=end_date,
        latitude=cfg.latitude,
        longitude=cfg.longitude,
    )
    return estimate_hourly_attendance(weather, max_capacity=cfg.max_capacity)


def estimate_hourly_revenue(
    attendance_df: pd.DataFrame,
    unit_revenue_huf: int,
    ratio_percent: float,
) -> pd.DataFrame:
    """Add hourly gross revenue estimate in HUF.

    At 100% ratio, gross hourly revenue baseline is unit_revenue_huf * 100.
    """
    ratio = max(0.0, ratio_percent) / 100.0
    hourly_gross_revenue_huf_at_100_ratio = max(0, int(unit_revenue_huf)) * 100
    df = attendance_df.copy()
    df["capacity_ratio"] = df["estimated_attendance"] / MAX_CAPACITY
    df["unit_revenue_huf"] = max(0, int(unit_revenue_huf))
    df["hourly_gross_revenue_huf_at_100_ratio"] = hourly_gross_revenue_huf_at_100_ratio
    df["estimated_hourly_gross_revenue_huf"] = (
        hourly_gross_revenue_huf_at_100_ratio * ratio * df["capacity_ratio"]
    ).round(0)
    return df


def create_dashboard_html(
    df: pd.DataFrame,
    output_html: str,
    *,
    default_unit_revenue_huf: int,
    ratio_percent: float,
) -> None:
    """Create a single dashboard HTML with weekly revenue chart and yearly summary table."""
    open_df = df[df["M_calendar"] > 0].copy()
    if open_df.empty:
        raise ValueError("No opening-hour rows available for dashboard generation.")

    open_df["year"] = open_df["timestamp"].dt.year
    open_df["iso_week"] = open_df["timestamp"].dt.isocalendar().week.astype(int)

    weekly = (
        open_df.groupby(["year", "iso_week"], as_index=False)
        .agg(weekly_avg_capacity_ratio=("capacity_ratio", "mean"))
        .sort_values(["year", "iso_week"])
    )
    weekly = weekly[weekly["weekly_avg_capacity_ratio"] > 0].copy()
    yearly = (
        open_df.groupby("year", as_index=False)
        .agg(yearly_capacity_ratio_sum=("capacity_ratio", "sum"))
        .sort_values("year")
    )

    payload = {
    "ratio_percent": float(ratio_percent),
    "default_unit_revenue_huf": int(default_unit_revenue_huf),
    "weekly": weekly.to_dict(orient="records"),
    "yearly": yearly.to_dict(orient="records"),
    }
    payload_json = json.dumps(payload, ensure_ascii=True)

    html = f"""<!doctype html>
<html lang=\"hu\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Budakeszi Revenue Dashboard</title>
    <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
    <style>
        :root {{
            --bg: #f6f7f4;
            --card: #ffffff;
            --text: #132238;
            --muted: #4f5f74;
            --line1: #005f73;
            --line2: #ee9b00;
            --border: #d7dde5;
        }}
        body {{ margin: 0; font-family: "Avenir Next", "Segoe UI", sans-serif; background: radial-gradient(circle at 10% 10%, #e8efe6, var(--bg) 40%); color: var(--text); }}
        .wrap {{ max-width: 1100px; margin: 0 auto; padding: 24px 16px 36px; }}
        .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px; box-shadow: 0 8px 22px rgba(8, 28, 44, 0.06); }}
        h1 {{ margin: 0 0 10px; font-size: 28px; letter-spacing: 0.2px; }}
        p {{ margin: 0; color: var(--muted); }}
        .controls {{ margin: 14px 0 18px; display: flex; gap: 14px; align-items: center; flex-wrap: wrap; }}
        label {{ font-weight: 600; }}
        input[type=number] {{ width: 120px; padding: 8px; border-radius: 8px; border: 1px solid var(--border); font-size: 16px; }}
        .meta {{ color: var(--muted); font-size: 14px; }}
        #chart {{ height: 500px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 15px; }}
        th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--border); text-align: right; }}
        th:first-child, td:first-child {{ text-align: left; }}
        th {{ background: #f7fafc; }}
        @media (max-width: 700px) {{ #chart {{ height: 420px; }} h1 {{ font-size: 23px; }} }}
    </style>
</head>
<body>
    <div class=\"wrap\">
        <div class=\"card\">
            <h1>Budakeszi heti átlagos bevétel</h1>
            <p>Külön vonalak évek szerint, nyitvatartási órák alapján.</p>
            <div class=\"controls\">
                <label for=\"unitInput\">Alap egységár (HUF)</label>
                <input id=\"unitInput\" type=\"number\" min=\"0\" step=\"1\" value=\"{int(default_unit_revenue_huf)}\" />
                <div class=\"meta\" id=\"hourlyBaseText\"></div>
            </div>
            <div id=\"chart\"></div>
            <table id=\"summaryTable\">
                <thead>
                    <tr>
                        <th>Év</th>
                        <th>Bruttó bevétel (HUF)</th>
                        <th>Nettó bevétel, 27% ÁFA nélkül (HUF)</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>
    </div>

    <script>
        const DATA = {payload_json};
        const MONTH_TICKS = [1, 5, 9, 14, 18, 22, 27, 31, 36, 40, 44, 49];
        const MONTH_NAMES_HU = [
            'jan', 'febr', 'márc', 'ápr', 'máj', 'jún',
            'júl', 'aug', 'szept', 'okt', 'nov', 'dec'
        ];

        function formatHUF(v) {{
            return new Intl.NumberFormat('hu-HU', {{ maximumFractionDigits: 0 }}).format(Math.round(v));
        }}

        function computeHourlyBase(unitValue) {{
            return Math.max(0, Math.floor(unitValue)) * 100;
        }}

        function render() {{
            const unitInput = document.getElementById('unitInput');
            const unitValue = Number(unitInput.value || 0);
            const hourlyBase = computeHourlyBase(unitValue);
            const ratio = Math.max(0, Number(DATA.ratio_percent)) / 100;

            document.getElementById('hourlyBaseText').textContent =
                `100% aránynál óránkénti bruttó: ${{formatHUF(hourlyBase)}} HUF`;

            const weeklyByYear = new Map();
            for (const row of DATA.weekly) {{
                const revenue = row.weekly_avg_capacity_ratio * hourlyBase * ratio;
                if (!weeklyByYear.has(row.year)) weeklyByYear.set(row.year, {{ x: [], y: [] }});
                weeklyByYear.get(row.year).x.push(row.iso_week);
                weeklyByYear.get(row.year).y.push(revenue);
            }}

            const traces = Array.from(weeklyByYear.entries()).map(([year, vals], idx) => {{
                const colors = ['#005f73', '#ee9b00', '#ca6702', '#0a9396'];
                return {{
                    x: vals.x,
                    y: vals.y,
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: `${{year}} heti átlagos bruttó`,
                    line: {{ width: 2.5, color: colors[idx % colors.length] }},
                    marker: {{ size: 5 }},
                }};
            }});

            Plotly.react('chart', traces, {{
                title: 'Heti átlagos bruttó bevétel évek szerint',
                template: 'plotly_white',
                hovermode: 'x unified',
                xaxis: {{
                    title: 'Hónap',
                    tickmode: 'array',
                    tickvals: MONTH_TICKS,
                    ticktext: MONTH_NAMES_HU,
                }},
                yaxis: {{ title: 'Heti átlagos bruttó bevétel (HUF)' }},
                legend: {{ orientation: 'h', yanchor: 'bottom', y: 1.02, x: 0 }},
                margin: {{ l: 60, r: 20, t: 60, b: 50 }},
            }}, {{ responsive: true }});

            const tbody = document.querySelector('#summaryTable tbody');
            tbody.innerHTML = '';
            for (const row of DATA.yearly) {{
                const gross = row.yearly_capacity_ratio_sum * hourlyBase * ratio;
                const net = gross / 1.27;
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${{row.year}}</td>
                    <td>${{formatHUF(gross)}}</td>
                    <td>${{formatHUF(net)}}</td>
                `;
                tbody.appendChild(tr);
            }}
        }}

        document.getElementById('unitInput').addEventListener('input', render);
        render();
    </script>
</body>
</html>
"""

    Path(output_html).parent.mkdir(parents=True, exist_ok=True)
    Path(output_html).write_text(html, encoding="utf-8")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate hourly attendance for Budakeszi outdoor events with weather heuristics."
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--unit-revenue-huf",
        type=int,
        default=2000,
        help="Base integer unit value in HUF. Hourly gross at 100%% equals this * 100. Default: 2000",
    )
    parser.add_argument(
        "--ratio-percent",
        type=float,
        default=100.0,
        help="Business ratio in percent. 100 means full ratio.",
    )
    parser.add_argument(
        "--csv-output",
        default="output/attendance_estimate.csv",
        help="CSV output path",
    )
    parser.add_argument(
        "--plot-output",
        default="output/attendance_chart.html",
        help="Interactive HTML chart output path",
    )
    return parser.parse_args(argv)


def _validate_dates(start_date_str: str, end_date_str: str) -> None:
    start = date.fromisoformat(start_date_str)
    end = date.fromisoformat(end_date_str)
    if end < start:
        raise ValueError("end_date must be >= start_date")


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    _validate_dates(args.start_date, args.end_date)

    attendance = build_attendance_dataset(args.start_date, args.end_date)
    result = estimate_hourly_revenue(
        attendance,
        unit_revenue_huf=args.unit_revenue_huf,
        ratio_percent=args.ratio_percent,
    )

    csv_path = Path(args.csv_output)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(csv_path, index=False)

    create_dashboard_html(
        result,
        args.plot_output,
        default_unit_revenue_huf=args.unit_revenue_huf,
        ratio_percent=args.ratio_percent,
    )

    print(f"Rows: {len(result)}")
    print(f"CSV saved to: {csv_path.resolve()}")
    print(f"Plot saved to: {Path(args.plot_output).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
