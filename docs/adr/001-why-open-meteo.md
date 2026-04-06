# ADR 001: Why Open-Meteo instead of OpenWeatherMap

## Status
Accepted

## Context
The pipeline requires hourly weather data for 50 global cities.

Each city requires:
- 24 data points per day (hourly)
- 50 cities × 24 = 1,200 API calls per day

OpenWeatherMap free tier:
- 1,000 calls/day limit
- Requires API key
- Rate limiting constraints

This would break the pipeline or require paid subscription.

## Decision
We use Open-Meteo API.

Reasons:
- No API key required
- No strict rate limits for non-commercial usage
- Provides hourly forecast data
- Includes 80+ years of historical weather data
- Aggregates data from trusted sources (NOAA, DWD, Météo-France)

## Consequences

### Positive
- Zero cost pipeline
- No authentication complexity
- Reliable for continuous hourly ingestion
- Supports both real-time and historical backfills

### Negative
- Less well-known than OpenWeatherMap (resume recognition slightly lower)
- Not suitable for commercial usage without proper licensing

## Conclusion
Open-Meteo is the optimal choice for a cost-free, scalable, and reliable weather data pipeline.