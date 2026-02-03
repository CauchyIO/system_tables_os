# Project Guidelines

## Git
- Only commit/push to feature branches
- Branch naming: `issue/{TEAM}-{NUMBER}` (e.g., `issue/CAU-75`)

## Code Style
- No comments
- Minimal print statements
- Hard fail (no try/except)
- Always use `uv run` instead of `python` 

## Configuration
- Widget parameters as defaults
- YAML as optional override
- Pydantic validation for all configs

## Structure
- `consolidate/` - Cross-account data aggregation
- `distribute/` - BU/workspace-specific views
- `utils.py` - Shared Pydantic models and helpers
