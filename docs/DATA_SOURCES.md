# Data Sources

| Source | Data | Important behavior |
| --- | --- | --- |
| SEC EDGAR | `filings.recent` submissions and XBRL Company Facts | CIK and filing provenance are retained; submissions are not complete filing history. |
| Twelve Data | Daily OHLCV by configured ticker | Market dates follow trading calendars; missing dates are not failures. |
| FRED | Series metadata and observations | Mixed frequencies, nullable `"."` observations, and source real-time metadata are retained. |

Initial companies are AAPL, MSFT, NVDA, AMZN, XOM, and WMT. Initial FRED
series are DFF, CPIAUCSL, UNRATE, GDPC1, and DGS10.

Provider credentials are environment-managed. Python validates required source
shape and identities before Bronze persistence; it does not infer universal
price ranges, FRED value ranges, filing cadences, or history completeness.
