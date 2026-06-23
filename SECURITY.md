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

## Supported versions

deltat is pre-1.0; fixes land on `main`. Pin a commit or tag for reproducible builds until the
wire/storage format is frozen for 1.0.
