# GitHub Actions in d4m

This directory contains CI/CD workflows for `d4m`:

- `workflows/build.yml` - regular build and test validation.
- `workflows/release.yml` - publish artifacts to Sonatype (snapshots and releases).

## Build Workflow

`build.yml` runs on:

- `pull_request` to `main`
- `push` to `main`
- manual trigger (`workflow_dispatch`)

What it does:

1. Checks out the repository.
2. Runs a JVM matrix with Temurin JDK versions `11`, `17`, `21`, and `25`.
3. Uses Gradle cache for each matrix job.
4. Runs:

```bash
./gradlew --no-daemon --stacktrace clean build
```

`build` is the whole definition of green here - unit and stress tests, Checkstyle and the JaCoCo report all hang off it.
There is no separate long-running suite to split out.

## Release and Publish Workflow

`release.yml` supports two publishing modes:

- **snapshot** - publish `*-SNAPSHOT` versions to Sonatype snapshots repository.
- **release** - publish release versions and finalize through Sonatype Central Portal.

The publish workflow runs on **JDK 11** to keep release artifacts built from the minimum supported Java baseline.

### Triggers

- manual trigger (`workflow_dispatch`) with `publish_mode` input (`snapshot` or `release`)
- git tag push matching `v*` (automatically uses `release` mode)

### Commands

- Snapshot mode:

```bash
./gradlew --no-daemon --stacktrace clean build publish
```

- Release mode:

```bash
./gradlew --no-daemon --stacktrace clean build publish uploadArtifactsToSonatypeCentralPortal
```

Both run `build` first - nothing is published which has not passed the tests, Checkstyle and JaCoCo of the
build itself, rather than trusting an earlier run on another commit.

## Published Artifacts

Group `io.github.green4j`:

| Artifact | What it is |
|---|---|
| `d4m-common` | shared primitives: `AtomicBuffer`, `UnsafeBuffer`, bit and byte utilities |
| `d4m-kv` | in-memory key-value map store |
| `d4m-sequence` | in-memory entry sequence store |
| `d4m-queue` | single-producer/single-consumer binary queue with spill to disk |
| `d4m-all` | the four above in one shaded jar |

Each carries a sources jar and a javadoc jar; `d4m-all` carries one javadoc over all four.
`d4m-benchmark` and `d4m-example` are not published.

## Required GitHub Secrets

Configure these secrets in repository or organization settings:

- `SONATYPE_USERNAME`
- `SONATYPE_PASSWORD`
- `SIGNING_GPG_SECRET_KEY` (ASCII-armored private key)
- `SIGNING_GPG_PASSWORD`

The Gradle build reads these from environment variables in `build.gradle`.

## Version Rules

Version is read from `version.txt`.

- Snapshot mode requires version ending with `-SNAPSHOT`.
- Release mode requires version **without** `-SNAPSHOT`.

The workflow validates this before running Gradle. A tag push is validated further: the tag has to be
`v<version>` of the same `version.txt`, so a tag names what was actually published.

## Recommended Release Procedure

1. Ensure `build.yml` is green on `main`.
2. Update `version.txt` to a non-snapshot version (for example `0.0.2`).
3. Push the release commit and create/push tag `v0.0.2` (or run `release.yml` manually with `publish_mode=release`).
4. Wait for `release.yml` to publish and finalize artifacts.
5. After release, bump `version.txt` to next snapshot (for example `0.0.3-SNAPSHOT`).

## An example of release

1. Set release version
```bash
echo 0.0.2 > version.txt
git add version.txt && git commit -m "Release 0.0.2"
git push
```
2. Initiate release pushing the tag
```bash
git tag v0.0.2
git push origin v0.0.2
```
3. Increment version to next snapshot
```bash
echo 0.0.3-SNAPSHOT > version.txt
git add version.txt && git commit -m "Back to snapshot" && git push
```

## Troubleshooting

- **Missing secrets**: release workflow fails early with a clear message.
- **Version/mode mismatch**: verify `version.txt` suffix and selected `publish_mode`.
- **Signing failures**: ensure the key is ASCII-armored and password matches the key.
- **Sonatype upload errors**: retry after verifying credentials and Sonatype account permissions.
- **Build run stuck in Queued and cannot be cancelled**: run `build.yml` manually on the same branch;
  its concurrency group cancels the stuck run.
