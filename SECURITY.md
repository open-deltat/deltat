# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately**, not as a public issue.

Use GitHub's private vulnerability reporting: open the repository's **Security** tab and choose
**Report a vulnerability**. This opens a private advisory visible only to the maintainers.

Please include:
- a description of the issue and its impact,
- the affected version or commit,
- steps to reproduce (a minimal SQL sequence or input is ideal), and
- any suggested fix.

We aim to acknowledge a report within a few days and to keep you updated as we work on a fix.
Please give us a reasonable window to address the issue before any public disclosure.

## Scope

deltat is a database that operates on untrusted input at its protocol boundary. Issues we
particularly care about:
- any input (SQL, parameters, or a crafted WAL) that can panic the server, exhaust memory, or
  otherwise cause denial of service,
- any path that returns incorrect availability or allows a double-booking past capacity,
- tenant isolation failures, and
- credential or secret exposure in logs or output.

## Trust model and hardening

deltat authenticates with cleartext passwords over the pgwire protocol. Know what that means
before exposing a port:

- **The global password is root on every tenant.** A connection picks its tenant by database
  name, and any holder of `DELTAT_PASSWORD` can read and write all of them and create new ones.
  Tenant separation (engine + WAL per tenant) is a storage layout, not an access boundary.
- **Scope credentials with `DELTAT_TENANT_PASSWORDS`** (comma-separated `tenant:password`
  pairs). A tenant listed there accepts only its own password; the global password then covers
  only unlisted tenants. This is the mechanism to use when distinct clients connect directly.
- **No default password.** If `DELTAT_PASSWORD` is unset, a random password is generated and
  printed once to stdout at startup; it changes on every restart. The docker-compose file
  refuses to start without an explicit password.
- **Auth is cleartext without TLS.** Set `DELTAT_TLS_CERT` and `DELTAT_TLS_KEY` for any
  non-loopback deployment, or every password crosses the network readable.
- **Prefer a private bind.** `DELTAT_BIND` defaults to `0.0.0.0`; bind `127.0.0.1` or a private
  interface unless the port must be reachable.

## Supported versions

deltat is pre-1.0; fixes land on `main`. Pin a commit or tag for reproducible builds until the
wire/storage format is frozen for 1.0.
