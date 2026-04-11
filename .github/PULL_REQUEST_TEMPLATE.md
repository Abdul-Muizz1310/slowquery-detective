# Pull Request

## Spec-TDD checklist

- [ ] Spec written under `docs/specs/` with enumerated test cases (success + failure)
- [ ] Red tests committed first, confirmed failing for the expected reason
- [ ] Implementation brings every test green
- [ ] Coverage ≥80% on touched `src/` files
- [ ] No untyped dicts cross module boundaries
- [ ] Every acceptance bullet from the source spec is observable in the diff

## Summary

<!-- Why this change, not what. -->

## Test plan

- [ ] `uv run pytest`
- [ ] `uv run ruff check .`
- [ ] `uv run ruff format --check .`
- [ ] `uv run mypy src/`
