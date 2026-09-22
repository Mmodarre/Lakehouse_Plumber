# Terms of Use — Lakehouse Plumber


**Last updated:** 2026-09-22

**Issued by:** Lakehouse Plumber maintainers team ("Licensor", "we", "us", or "our").

These Terms of Use govern your access to and use of the Lakehouse Plumber software, including the `lhp` command-line application, the LHP Web IDE, templates, documentation, and any related materials we make available (together, "LHP").

## 1. Agreement to these Terms

By downloading, installing, copying, executing, or otherwise using LHP, or by accessing the LHP Web IDE, you agree to these Terms. If you do not agree, do not use LHP.

The source code is separately licensed under the Apache License 2.0. Nothing in these Terms limits the permissions granted by that license. Where a provision in these Terms concerns the use of a hosted service, web interface, telemetry endpoint, or other non-source material, these Terms supplement the license.

## 2. What LHP is and is not

LHP is a code-generation tool. It converts configuration files into Python code intended for Databricks Lakeflow Declarative Pipelines.

LHP does not run, deploy, monitor, or operate your pipelines. Unless we install, configure, or execute something in your environment at your explicit direction, the generated code and every Databricks workload, cluster, job, database, or other resource remain under your control and your responsibility.

## 3. "As is" and no warranty

LHP is provided on an "AS IS" and "AS AVAILABLE" basis, without warranties or conditions of any kind, whether express, implied, statutory, or otherwise. To the maximum extent permitted by applicable law, we disclaim all warranties, including any implied warranties of merchantability, fitness for a particular purpose, title, non-infringement, accuracy, reliability, security, and uninterrupted or error-free operation.

We do not warrant that:

- LHP will satisfy your requirements or be suitable for your environment;
- operation of LHP will be uninterrupted, secure, or free from defects;
- generated code will be correct, complete, production-ready, or compatible with every Databricks version, API, or policy; or
- any documentation, examples, templates, presets, or AI-assistant output will be accurate.

## 4. Your responsibilities

You are solely responsible for all consequences of your use of LHP, including:

- reviewing, testing, and validating every generated file and configuration before deploying or executing it;
- ensuring that your use complies with applicable law and with your agreements with third parties, including Databricks and your cloud providers;
- obtaining all necessary rights, licenses, permissions, and approvals for your data, accounts, credentials, secrets, and cloud resources;
- reviewing, approving, and supervising the actions of the LHP Web IDE and any AI assistant, including commands, file changes, and external service requests; and
- maintaining appropriate backups, access controls, security controls, and recovery plans.

You acknowledge that AI-generated or template-generated output can be incorrect, incomplete, unsafe, or incompatible with your environment, and that you must review it before use.

## 5. Third-party services

LHP may interact with third-party products and services, including Databricks, Anthropic or Claude, Omnigent, and their APIs. We do not own or control those services.

Those services:

- have their own terms, privacy policies, fees, quotas, and availability;
- may receive, store, or process data you submit or authorize LHP to submit; and
- may change or discontinue functionality without notice.

We are not a party to your agreements with those providers and are not responsible for their services, charges, data practices, availability, defects, or the results they produce.

## 6. Telemetry

Anonymous usage telemetry is on by default. LHP records command, web-session, and run events without collecting names, file paths, file contents, SQL, Python, configuration values, or error messages. You can disable telemetry with `LHP_TELEMETRY=off`, `DO_NOT_TRACK=1`, or `lhp telemetry off`.

See the [telemetry reference](docs/reference/telemetry.rst) for exactly what is collected and the available opt-outs. If you require a separate privacy policy, a privacy statement should be published alongside these Terms.

## 7. Limitation of liability

To the maximum extent permitted by applicable law, and except to the extent such liability cannot be limited by law, we will not be liable to you or any other person for any indirect, incidental, special, consequential, exemplary, or punitive damages, or for any loss of profits, revenue, business opportunity, goodwill, data, or use, whether arising in contract, tort (including negligence), statute, or otherwise, even if advised of the possibility of such damage.

To the maximum extent permitted by applicable law, our total aggregate liability arising out of or relating to LHP will not exceed the amount actually paid by you for LHP during the twelve months before the event giving rise to the claim. If you have paid nothing, no monetary remedy is available under this section.

These limitations apply to the use of LHP, the generated code, the LHP Web IDE, the AI assistant, documents, templates, and telemetry.

## 8. Indemnification

You agree to defend, indemnify, and hold harmless the Licensor and contributors from claims, damages, losses, and reasonable expenses arising out of your breach of these Terms, your use of LHP, or your unlawful or unauthorized use of third-party services or data.

## 9. Disclaimer about legal protection

The "as is" disclaimer, warranty disclaimer, and liability cap reduce ordinary civil exposure, but they cannot eliminate all legal liability. In most jurisdictions they do not protect against fraud, willful misconduct, gross negligence where liability is mandatory, death or personal injury where liability cannot be excluded, or regulatory and criminal proceedings arising from unlawful activity.

## 10. Changes, termination, and survival

We may update these Terms from time to time. Continued use after an update means you accept the updated Terms. The sections concerning warranty disclaimers, responsibility, third-party services, limitation of liability, indemnification, and governing law survive termination.

## 11. Governing law

These Terms are governed by the laws of Commonwealth of Australia, without regard to its conflict-of-law rules. You and we submit to the exclusive jurisdiction of the courts located in Australia.
