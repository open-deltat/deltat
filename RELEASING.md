# Releasing

Two artifacts go out per release: the crate on crates.io and the container image on
ghcr.io. The image is automated; the crate is not, because `cargo publish` cannot be undone.
A published version can only be yanked, never replaced, so it stays a deliberate act.

## Steps

1. Bump `version` in `Cargo.toml`, then `cargo check` so `Cargo.lock` records the same version.
   A stale lockfile makes `cargo publish` refuse to run against a dirty tree.
2. Move the `## [Unreleased]` entries in `CHANGELOG.md` under a new `## [x.y.z] - YYYY-MM-DD`
   heading and leave `[Unreleased]` empty above it.
3. Bump the pinned tag in `docker-compose.yml` and in the README quick start.
4. Open a PR, let CI pass, merge.
5. From `main`: `cargo publish`.
6. Tag and push: `git tag -a vx.y.z -m "..." && git push origin vx.y.z`. That fires
   `.github/workflows/release.yml`, which builds `linux/amd64` and `linux/arm64` on native
   runners and publishes one multi-architecture tag.
7. `gh release create vx.y.z` with notes drawn from the changelog.
8. Bump `DELTAT_CRATE_VERSION` in the tap repo's `.github/workflows/ci.yml` so the SDK's
   contract suite runs against the release it ships against.

## One-time setup, already done

The container package must be made public once, by hand, at
`github.com/orgs/open-deltat/packages`. GitHub creates container packages private and exposes
no API for changing that. Later releases inherit the setting.

## Checking the pipeline without cutting a release

The release workflow accepts `workflow_dispatch`. Run it manually and it builds both
architectures and pushes nothing, which is enough to catch a broken Dockerfile or a runner
label that has been renamed.

## If the storage format changed

Bump `FORMAT_VERSION` in `src/wal.rs` in the same release. An older binary then refuses to open
the newer log instead of misreading it, and the changelog entry should say plainly what happens
to data written by the previous version.
