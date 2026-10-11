# Security policy

## Supported versions

k2kit is pre-1.0. Security fixes go into the latest minor release only.

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |

## Reporting a vulnerability

**Please don't open a public issue.** Report it privately through GitHub: on the repository, go to **Security → Report a vulnerability** ([direct link](https://github.com/sarmaasis/k2kit/security/advisories/new)).

Include what you found, how to reproduce it, and the impact you expect. You'll get an acknowledgement within 3 working days and an assessment within 10. Fixes are released as soon as they're ready, with a GitHub security advisory crediting you unless you prefer otherwise.

## What's in scope

k2kit handles Cloudflare API tokens and stream data, so these matter most:

- The API token appearing in logs, error messages, CLI output or anything persisted.
- Record contents leaking into logs (k2kit never logs them by design).
- Records being acked without being processed, or dropped without reaching the dead-letter handler.
- Temporary subscriptions created by the CLI not being cleaned up in a way that exposes data.

Vulnerabilities in Cloudflare K2 itself should go to [Cloudflare's disclosure program](https://www.cloudflare.com/disclosure/).

## Good practice for users

- Give each token only the permissions it needs: K2 Consume for consumers, K2 Produce for producers.
- Keep tokens in environment variables or a secret manager, never in code.
- `deadLetterToStream` stores your handler's error message in the `k2kit-dlq-error` header, so keep secrets and personal data out of error messages.
