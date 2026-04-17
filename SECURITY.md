# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.7.x   | :white_check_mark: |
| < 0.7   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in Lakehouse Plumber, please report it responsibly:

1. **Do NOT open a public GitHub issue** for security vulnerabilities.
2. Email the maintainers with:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact assessment
   - Suggested fix (if any)
3. You will receive an acknowledgment within 48 hours.
4. A fix will be developed and released as a patch version.

## Security Measures

This project employs the following security practices:

- **Dependency scanning**: `pip-audit` (OSV database) runs on every PR and weekly
- **Static analysis**: `bandit` scans Python source for common security issues
- **Secret scanning**: `gitleaks` checks for accidentally committed credentials
- **License compliance**: `liccheck` verifies all dependencies use approved licenses
- **Supply chain integrity**: GitHub Actions pinned to SHA hashes, OIDC trusted publishing to PyPI
- **Code review**: All changes to `main` require pull request review (CODEOWNERS enforced)
- **OpenSSF Scorecard**: Weekly automated security posture assessment

## Vulnerability Exceptions

Documented exceptions for known vulnerabilities that do not apply to this project:

<!-- Template for adding exceptions:
### GHSA-xxxx-xxxx-xxxx (CVE-YYYY-NNNNN)
- **Package**: package-name X.Y.Z
- **Reason**: [Why this vulnerability does not affect LHP]
- **Added**: YYYY-MM-DD
- **Review by**: YYYY-MM-DD
-->

*No current exceptions.*
