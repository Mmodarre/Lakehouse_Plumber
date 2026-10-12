# Contributing to Lakehouse Plumber

Thanks for your interest in contributing. By participating, you agree that all
contributions are licensed under the project's
[Apache License 2.0](LICENSE).

## Developer Certificate of Origin (DCO)

Every contribution must be signed off. The sign-off certifies that you wrote
the code or otherwise have the right to submit it under the project's license.

To sign off, commit with `-s`:

```bash
git commit -s -m "Add my feature"
```

This appends a line to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

Use your real name and a real email address. Pseudonymous or anonymous
sign-offs are not accepted.

By adding this sign-off, you certify the following:

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

If you forgot to sign off a commit, you can amend it:

```bash
git commit --amend -s --no-edit
```

Pull requests containing unsigned commits will be asked to add the sign-off
before merge.

## Agent guides and offline releases

The shared setup text lives in
`src/lhp/resources/skills/lhp/references/setup.md`. After changing it, run
`python scripts/render_agent_guides.py` to regenerate both self-contained prompts
in `docs/_agent_guides/`. CI checks that these copies agree. Sphinx embeds their
complete prompt bodies in the docs agent buttons; do not maintain separate prompt
text in JavaScript. The ``lhp-learn`` curriculum lives in
``src/lhp/resources/skills/lhp_learn/``; technical references remain in
``src/lhp/resources/skills/lhp/references/`` and are assembled into both exports.
Keep teaching instructions out of the development entrypoint. Teaching references,
technical references and the sample project ship in the same LHP distribution.
Both prompts try pip first. When that cannot provide a usable installation, they
select the complete platform/Python ZIP from GitHub Releases, including its pinned
dependencies and installer. Keep this fallback consistent in both skill exports;
the standalone wheel is not the offline download recommendation.

Offline runtime versions come from `uv.lock`, including the optional `genie`
dependency group, without `dev` or `webapp` extras. Use `uv lock` when changing
dependencies or the project version, and commit the lockfile. `uv lock --check`
must pass; a stale lock is not silently refreshed during release. Keep registry
URLs public. Build-backend versions are pinned separately in
`scripts/release-build-requirements.txt`.

`Tests` calls the offline matrix on pull requests and main: CPython 3.12,
Linux x86_64/ARM64, macOS Apple Silicon and Windows x86_64 (4 jobs). Each job
builds an archive, installs it into a fresh environment with network access denied
inside pip, checks dependencies, exports both skills, and validates/generates the
packaged sample. The release repeats this matrix using the single canonical wheel
that will also go to PyPI. Native Genie discovery requires a separate Databricks
workspace acceptance test; mocked SDK checks cannot prove it.

`Build and Publish to PyPI` runs after successful main CI, pins every checkout to
the tested commit, and serializes releases. Manual dispatch also requires successful
main-branch CI for that exact commit. A version already released from an older
commit is skipped; bump the version to publish new work. Package-index errors
other than an explicit 404 stop publication.

After every archive passes, CI collects 4 ZIPs, their checksums, the wheel and
sdist, creates `SHA256SUMS`, attests the artifacts, and uploads them to a draft
GitHub release. PyPI uses OIDC trusted publishing. Only after PyPI succeeds is the
GitHub release made public. Existing assets are compared by hash and never blindly
overwritten. Retry an interrupted release with **Re-run all jobs** or manual
dispatch on the same commit; a different commit requires a new version. Configure
the repository's PyPI trusted publisher for `.github/workflows/publish.yml` and
permit the workflow's scoped release/attestation permissions.

Offline archives exclude LHP Web dependencies and do not include Python itself.
Linux bundles target glibc-based systems; each archive's native wheels determine
its minimum OS compatibility. Windows ARM64 is not advertised because the locked
SDK dependency set does not provide a complete native wheel set for that target.
