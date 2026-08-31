# Prompts

Prompts are tracked as research artifacts. Each LLM-derived annotation should record
the prompt file and version that produced it, along with provider, model, parameters,
timestamp, and status metadata.

Initial prompt files preserve methodology recovered from the historical notebooks.
Revisions should be added as new versioned files rather than overwriting historical wording.

The initial checkpoint creates reserved prompt files only. Historical wording will be
extracted during the LLM migration stage, when the related code can also record prompt
name/version on every annotation.

Prospective prompts implementing the frozen revised protocol are separate artifacts:

- `revised_star_title_abstract_screening_v1_0_0.md`
- `revised_star_full_text_screening_v1_0_0.md`

These do not replace or reinterpret the reserved historical prompt files.
