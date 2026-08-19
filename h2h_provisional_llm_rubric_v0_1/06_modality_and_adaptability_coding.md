# Prompt 06 — Modality and Adaptability Coding v0.1

Status: **PROVISIONAL**

These are descriptive attributes, not measures of agency.

## Visualization Modality

Question: **Through what perceptual/display environment is analytic information presented?**

Codes:
- `VM-DESKTOP2D`
- `VM-DESKTOP3D`
- `VM-LARGE_DISPLAY`
- `VM-MOBILE_TABLET`
- `VM-VR`
- `VM-AR_MR`
- `VM-CAVE`
- `VM-PHYSICAL_HAPTIC`
- `VM-NONVISUAL_OR_UNSPECIFIED`
- `VM-OTHER`

## Interaction Modality

Question: **Through what channel does the analyst communicate intent to the system?**

Codes:
- `IM-POINTER_KEYBOARD`
- `IM-TOUCH`
- `IM-SPEECH`
- `IM-TYPED_NATURAL_LANGUAGE`
- `IM-GESTURE`
- `IM-GAZE`
- `IM-TRACKED_CONTROLLER`
- `IM-EMBODIED_LOCOMOTION`
- `IM-HAPTIC`
- `IM-MULTIMODAL`
- `IM-OTHER`
- `IM-UNSPECIFIED`

Interaction modality is not agency.

## Adaptability — provisional

Question: **Does the mechanism change behavior as a function of user, context, history, or ongoing interaction?**

- `A0` Static
- `A1` Reactive/context-responsive
- `A2` Personalized/user-adaptive
- `A3` Co-adaptive/learning
- `A-UNCERTAIN`

## Output JSON

```json
{
  "paper_id": "",
  "mechanism_id": "",
  "visualization_modality": {
    "primary": "",
    "secondary": [],
    "evidence": [],
    "confidence": "LOW | MEDIUM | HIGH"
  },
  "interaction_modality": {
    "modalities": [],
    "evidence": [],
    "confidence": "LOW | MEDIUM | HIGH"
  },
  "adaptability": {
    "code": "A0 | A1 | A2 | A3 | A-UNCERTAIN",
    "evidence": [],
    "reasoning_summary": "",
    "confidence": "LOW | MEDIUM | HIGH"
  }
}
```

## Tie-breaks

- Speech or natural language does not imply high Delegation.
- Immersive display does not imply deep Mediation.
- Adaptability is responsiveness, not autonomy.
