# Validation record

Executed against the existing local dependency installation on 9 September 2026.

| Check | Result |
|---|---|
| `npm run build` in `web_app` | Pass; includes `tsc -b`. Installed Vite reports 7.3.1. |
| `npm run lint` | Pass. |
| `npm run size` | Pass: 148.45 kB app entry / 1.85 MB total JavaScript, Brotli. |
| `npm run test -- --run` | Initial local-environment failure: 356 failed, 1,205 passed; Node experimental web storage interferes with localStorage. |
| `NODE_OPTIONS=--no-experimental-webstorage npm run test -- --run` | Pass: 164 files / 1,561 tests. |
| Temporary audit probes under the same Node option | Pass: 9 probes reproduce current defective behavior. |
| Live dev server | Unavailable: listener rejected with EPERM. |
| Backend | Unavailable in the repo virtualenv: uvicorn is not installed. |
| Installed Chromium | Launch did not remain running; no live visual results claimed. |

The application manifest and installed packages are not identical (for example, manifest Vite ^8.1.5 versus installed Vite 7.3.1). A clean lockfile install and browser verification remain part of implementation acceptance. No dependencies were installed or upgraded during this review.

## Reproducing the audit probes

`audit-probes.tsx.txt` is an isolated review artifact. It deliberately asserts the current incorrect outcomes to make the diagnosis reproducible. Convert these to desired-behavior regressions when implementing fixes.

From the repository root, temporarily copy it into the original test location:

```sh
cp docs/reviews/lhp-frontend-audit-2026-09-09/audit-probes.tsx.txt web_app/src/test/frontend-audit.probe.test.tsx
```

From `web_app`, run:

```sh
NODE_OPTIONS=--no-experimental-webstorage npm run test -- --run src/test/frontend-audit.probe.test.tsx
```

Then remove that temporary copy. The report's final state leaves no new test or application source in `web_app/src`.

Probe scope: API calls are mocked; the run tests exercise the actual controller, transport lifecycle, and store with controlled pending responses. The New file probe confirms the unconditional empty write; the backend file-route source establishes that an absent conditional header permits overwriting an existing file. No user files were overwritten to reproduce it.
