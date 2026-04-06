# ADR 002: Why MinIO instead of AWS S3

## Status
Accepted

## Context
The pipeline requires object storage for raw and staged weather data.

Requirements:
- Store raw JSON data from API
- Support partitioned data layout (by city/date/hour)
- Be compatible with S3 APIs (for future scalability)
- Work locally without cloud dependencies
- Zero cost

Option considered:
- AWS S3 (industry standard)
- MinIO (S3-compatible local storage)

## Decision
We use MinIO as the object storage layer.

## Reasons

### 1. Cost
- AWS S3 requires a cloud account and billing setup
- Even with free tier, usage is limited and not predictable
- MinIO runs locally with zero cost

### 2. Local Development
- Entire pipeline runs via Docker Compose
- No external dependencies required
- Easier to reproduce and test

### 3. S3 Compatibility
- MinIO is fully S3-compatible
- Same APIs (PUT, GET, LIST)
- Easy to migrate to AWS S3 in production

### 4. Simplicity
- No IAM roles, credentials, or permissions complexity
- Faster setup and onboarding

## Consequences

### Positive
- Fully local, reproducible system
- Zero infrastructure cost
- Easier debugging and testing
- Faster development cycle

### Negative
- Not a managed service (no automatic scaling)
- No built-in redundancy like AWS S3
- Requires manual management in production

## Future Consideration
If deployed to production, MinIO can be replaced with AWS S3 with minimal code changes due to API compatibility.

## Conclusion
MinIO provides the best balance between cost, simplicity, and S3 compatibility for a local-first data platform.