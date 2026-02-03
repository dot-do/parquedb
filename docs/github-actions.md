# GitHub Actions for ParqueDB

This guide covers how to integrate ParqueDB with GitHub Actions for automated database CI/CD workflows. ParqueDB provides a setup action, CLI commands designed for CI environments, and pre-built workflow templates.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Setup Action](#setup-action)
- [CI Commands Reference](#ci-commands-reference)
- [Workflow Templates](#workflow-templates)
- [Configuration](#configuration)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

Add ParqueDB to your GitHub Actions workflow in 3 steps:

### 1. Add Repository Secrets

Go to **Settings > Secrets and variables > Actions** and add:

| Secret | Description |
|--------|-------------|
| `PARQUEDB_TOKEN` | Authentication token from [oauth.do](https://oauth.do) |
| `PARQUEDB_REMOTE_URL` | (Optional) Custom remote URL for your database |

### 2. Create a Workflow File

Create `.github/workflows/parquedb.yml`:

```yaml
name: ParqueDB CI

on:
  pull_request:
    types: [opened, synchronize, closed]

jobs:
  database-preview:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Create preview branch
        if: github.event.action != 'closed'
        run: parquedb ci preview create --pr ${{ github.event.pull_request.number }}

      - name: Generate diff
        if: github.event.action != 'closed'
        run: parquedb ci diff --base ${{ github.base_ref }} --head ${{ github.head_ref }}

      - name: Cleanup preview
        if: github.event.action == 'closed'
        run: parquedb ci preview delete --pr ${{ github.event.pull_request.number }}
```

### 3. Open a Pull Request

The workflow will automatically:
- Create an isolated preview database branch
- Show database changes in the PR
- Clean up when the PR is merged or closed

---

## Setup Action

The `parquedb/setup-action@v1` action installs and configures the ParqueDB CLI.

### Inputs

| Input | Description | Required | Default |
|-------|-------------|----------|---------|
| `version` | ParqueDB CLI version to install | No | `latest` |
| `token` | Authentication token from oauth.do | No | `''` |

### Outputs

| Output | Description |
|--------|-------------|
| `parquedb-version` | The installed CLI version |
| `cache-hit` | Whether the CLI was restored from cache (`true`/`false`) |

### Usage Examples

**Install latest version:**

```yaml
- uses: parquedb/setup-action@v1
```

**Install specific version:**

```yaml
- uses: parquedb/setup-action@v1
  with:
    version: '1.2.3'
```

**With authentication:**

```yaml
- uses: parquedb/setup-action@v1
  with:
    token: ${{ secrets.PARQUEDB_TOKEN }}
```

### Caching

The action automatically caches the CLI binary between runs. Subsequent runs will restore from cache unless the version changes.

### Supported Platforms

| Platform | Architecture |
|----------|--------------|
| Linux | x64 |
| macOS | x64, arm64 |
| Windows | x64 |

---

## CI Commands Reference

ParqueDB provides dedicated CI commands that automatically detect the CI environment and use appropriate defaults.

### Environment Detection

The CLI automatically detects these CI providers:
- GitHub Actions
- GitLab CI
- CircleCI

Use `parquedb ci setup` to verify detection:

```bash
$ parquedb ci setup
CI Provider: GitHub Actions
Repository: owner/repo
Branch: feature/my-branch
PR Number: 123
Base: main
Head: feature/my-branch

Non-interactive mode: enabled

Configuration:
- Color output: disabled
- Interactive prompts: disabled
```

### Command: `parquedb ci setup`

Detect CI environment and display configuration.

```bash
parquedb ci setup [--json]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--json` | Output as JSON |

**Example JSON output:**

```json
{
  "provider": "GitHub Actions",
  "repository": "owner/repo",
  "branch": "feature/branch",
  "prNumber": "123",
  "baseBranch": "main",
  "headBranch": "feature/branch",
  "nonInteractive": true,
  "colorOutput": false
}
```

---

### Command: `parquedb ci preview create`

Create or update a preview database branch for a pull request.

```bash
parquedb ci preview create [--pr <number>] [--branch <name>] [--json]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--pr <number>` | PR/MR number (auto-detected from CI) |
| `--branch <name>` | Custom branch name (defaults to `pr-<number>`) |
| `--json` | Output as JSON |

**Example output:**

```
Creating preview branch: pr-123
Pushing with visibility: unlisted
::set-output name=preview_url::https://preview.parquedb.com/owner/repo/pr-123
```

**JSON output includes:**

```json
{
  "branch": "pr-123",
  "url": "https://preview.parquedb.com/owner/repo/pr-123",
  "visibility": "unlisted",
  "pr": 123,
  "exists": false
}
```

---

### Command: `parquedb ci preview delete`

Delete a preview database branch.

```bash
parquedb ci preview delete [--pr <number>] [--branch <name>]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--pr <number>` | PR/MR number (auto-detected from CI) |
| `--branch <name>` | Custom branch name |

---

### Command: `parquedb ci diff`

Generate a diff between two database branches.

```bash
parquedb ci diff [--base <branch>] [--head <branch>] [--format <format>] [--summary]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--base <branch>` | Base branch (auto-detected from CI) |
| `--head <branch>` | Head branch (auto-detected from CI) |
| `--format <format>` | Output format: `text`, `json`, or `markdown` (default: `text`) |
| `--summary` | Write to `GITHUB_STEP_SUMMARY` |

**Text output example:**

```
Comparing: main...feature/users
users: +5 -2 ~3
posts: +10 -0 ~1
Total: +15 -2 ~4
```

**Markdown output example:**

```markdown
| Collection | Added | Removed | Modified |
| --- | --- | --- | --- |
| users | +5 | -2 | ~3 |
| posts | +10 | -0 | ~1 |

Total: +15 -2 ~4

**Summary**
2 collections affected
```

**JSON output example:**

```json
{
  "base": "main",
  "head": "feature/users",
  "collections": [
    { "name": "users", "added": 5, "removed": 2, "modified": 3 },
    { "name": "posts", "added": 10, "removed": 0, "modified": 1 }
  ],
  "totals": { "added": 15, "removed": 2, "modified": 4 }
}
```

---

### Command: `parquedb ci check-merge`

Check for merge conflicts between branches.

```bash
parquedb ci check-merge [--base <branch>] [--head <branch>] [--strategy <strategy>] [--verbose] [--json]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--base <branch>` | Base branch (auto-detected from CI) |
| `--head <branch>` | Head branch (auto-detected from CI) |
| `--strategy <strategy>` | Conflict resolution strategy: `manual`, `ours`, `theirs`, `newest` (default: `manual`) |
| `--verbose` | Show detailed conflict information |
| `--json` | Output as JSON |

**Exit codes:**

| Code | Meaning |
|------|---------|
| `0` | No conflicts |
| `1` | Conflicts detected |

**Example output (no conflicts):**

```
No conflicts detected
```

**Example output (conflicts):**

```
Conflicts detected:
  - users/user-123
  - posts/post-456
2 conflicts found
```

**Verbose output:**

```
Conflicts detected:
  Entity ID: users/user-123
  Collection: users
  Conflict type: concurrent-update

  Entity ID: posts/post-456
  Collection: posts
  Conflict type: delete-update

2 conflicts found
```

---

## Workflow Templates

ParqueDB provides ready-to-use workflow templates in `.github/workflows/templates/`.

### PR Preview Database

**File:** `.github/workflows/templates/pr-preview.yml`

Creates isolated preview branches for pull requests and cleans them up automatically.

**Features:**
- Creates preview branch on PR open/sync
- Posts preview URL as PR comment
- Deletes preview branch on PR close

```yaml
name: PR Preview Database

on:
  pull_request:
    types: [opened, synchronize, closed]

concurrency:
  group: pr-preview-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  preview:
    name: Create/Update Preview
    runs-on: ubuntu-latest
    if: github.event.action != 'closed'
    steps:
      - uses: actions/checkout@v4

      - uses: parquedb/setup-action@v1

      - name: Create preview branch
        run: parquedb ci preview create --pr ${{ github.event.pull_request.number }}
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Post preview URL comment
        uses: actions/github-script@v7
        with:
          script: |
            const prNumber = context.payload.pull_request.number;
            const previewUrl = `https://preview-${prNumber}.parquedb.dev`;
            await github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: prNumber,
              body: `## Preview Database\n\nYour preview database is ready at: ${previewUrl}`
            });

  cleanup:
    name: Cleanup Preview
    runs-on: ubuntu-latest
    if: github.event.action == 'closed'
    steps:
      - uses: actions/checkout@v4

      - uses: parquedb/setup-action@v1

      - name: Delete preview branch
        run: parquedb ci preview delete --pr ${{ github.event.pull_request.number }}
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}
```

---

### Database Diff

**File:** `.github/workflows/templates/database-diff.yml`

Shows database changes as a PR comment, updating on each push.

**Features:**
- Generates diff between PR branches
- Posts/updates diff as PR comment
- Handles concurrent runs gracefully

```yaml
name: Database Diff

on:
  pull_request:

concurrency:
  group: database-diff-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  diff:
    name: Generate Database Diff
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: parquedb/setup-action@v1

      - name: Run database diff
        id: diff
        run: |
          parquedb ci diff > diff_output.txt
          echo "diff<<EOF" >> $GITHUB_OUTPUT
          cat diff_output.txt >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Find existing comment
        id: find-comment
        uses: actions/github-script@v7
        with:
          script: |
            const comments = await github.rest.issues.listComments({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.payload.pull_request.number
            });
            const botComment = comments.data.find(comment =>
              comment.user.type === 'Bot' &&
              comment.body.includes('## Database Diff')
            );
            return botComment ? botComment.id : null;

      - name: Update or create comment
        uses: actions/github-script@v7
        with:
          script: |
            const diff = `${{ steps.diff.outputs.diff }}`;
            const body = `## Database Diff\n\n\`\`\`diff\n${diff}\n\`\`\``;
            const commentId = ${{ steps.find-comment.outputs.result }};

            if (commentId) {
              await github.rest.issues.updateComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                comment_id: commentId,
                body: body
              });
            } else {
              await github.rest.issues.createComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                issue_number: context.payload.pull_request.number,
                body: body
              });
            }
```

---

### Merge Check

**File:** `.github/workflows/templates/merge-check.yml`

Validates database branches can merge without conflicts.

**Features:**
- Creates GitHub Check Run for visibility
- Blocks PR merge if conflicts exist
- Updates check status on completion

**Configuring as Required Check:**

1. Go to repository **Settings > Branches**
2. Add branch protection rule for `main`
3. Enable "Require status checks to pass before merging"
4. Add "ParqueDB Merge Check" to required checks

```yaml
name: Merge Check

on:
  pull_request:

concurrency:
  group: merge-check-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  check:
    name: Check Merge Conflicts
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: parquedb/setup-action@v1

      - name: Create check run
        uses: actions/github-script@v7
        id: check
        with:
          check-name: ParqueDB Merge Check
          script: |
            const check = await github.rest.checks.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              name: 'ParqueDB Merge Check',
              head_sha: context.payload.pull_request.head.sha,
              status: 'in_progress'
            });
            return check.data.id;

      - name: Run merge check
        id: merge-check
        run: parquedb ci check-merge
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Update check run status
        if: always()
        uses: actions/github-script@v7
        with:
          script: |
            const conclusion = '${{ steps.merge-check.outcome }}' === 'success' ? 'success' : 'failure';
            await github.rest.checks.update({
              owner: context.repo.owner,
              repo: context.repo.repo,
              check_run_id: ${{ steps.check.outputs.result }},
              status: 'completed',
              conclusion: conclusion
            });
```

---

### Schema Check

**File:** `.github/workflows/templates/schema-check.yml`

Validates schema changes and warns about breaking changes.

**Features:**
- Triggers only when `parquedb.config.ts` changes
- Detects breaking schema changes
- Posts schema diff as PR comment with warning if breaking

```yaml
name: Schema Check

on:
  pull_request:
    paths:
      - parquedb.config.ts

concurrency:
  group: schema-check-${{ github.event.pull_request.number }}
  cancel-in-progress: true

jobs:
  schema:
    name: Check Schema Changes
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: parquedb/setup-action@v1

      - name: Run schema diff
        id: schema-diff
        run: |
          parquedb schema diff > schema_diff.txt
          echo "diff<<EOF" >> $GITHUB_OUTPUT
          cat schema_diff.txt >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Check for breaking changes
        id: breaking
        run: |
          if parquedb schema diff --check-breaking; then
            echo "has_breaking=false" >> $GITHUB_OUTPUT
          else
            echo "has_breaking=true" >> $GITHUB_OUTPUT
          fi
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Post schema changes comment
        uses: actions/github-script@v7
        with:
          script: |
            const diff = `${{ steps.schema-diff.outputs.diff }}`;
            const hasBreaking = '${{ steps.breaking.outputs.has_breaking }}' === 'true';

            let body = '## Schema Changes\n\n';
            if (hasBreaking) {
              body += '> **Warning**: This PR contains breaking schema changes!\n\n';
            }
            body += '```diff\n' + diff + '\n```';

            await github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.payload.pull_request.number,
              body: body
            });
```

---

### Auto Merge Database

**File:** `.github/workflows/templates/auto-merge.yml`

Automatically merges database branches when PRs are merged.

**Features:**
- Triggers only when PR is merged (not just closed)
- Merges preview database branch into main
- Pushes changes and cleans up preview branch

```yaml
name: Auto Merge Database

on:
  pull_request:
    types: [closed]

concurrency:
  group: auto-merge-${{ github.event.pull_request.number }}
  cancel-in-progress: false

jobs:
  merge:
    name: Merge Database Branch
    runs-on: ubuntu-latest
    if: github.event.pull_request.merged == true
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: parquedb/setup-action@v1

      - name: Merge database branch
        run: parquedb merge --branch preview-${{ github.event.pull_request.number }}
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Push merged changes
        run: parquedb push
        env:
          PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Cleanup preview branch
        run: |
          git push --delete origin preview-${{ github.event.pull_request.number }} || true
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

---

## Configuration

### Environment Variables

The CLI recognizes these environment variables in CI:

#### GitHub Actions Variables

| Variable | Description |
|----------|-------------|
| `PARQUEDB_TOKEN` | Authentication token |
| `PARQUEDB_REMOTE_URL` | Custom remote URL |
| `GITHUB_ACTIONS` | Detected automatically (set to `true`) |
| `GITHUB_REPOSITORY` | Repository name (owner/repo) |
| `GITHUB_REF_NAME` | Current branch name |
| `GITHUB_BASE_REF` | PR base branch |
| `GITHUB_HEAD_REF` | PR head branch |
| `GITHUB_EVENT_NUMBER` | PR number |
| `GITHUB_WORKFLOW` | Workflow name |
| `GITHUB_STEP_SUMMARY` | Path for job summary output |

#### GitLab CI Variables

| Variable | Description |
|----------|-------------|
| `GITLAB_CI` | Detected automatically |
| `CI_PROJECT_PATH` | Project path (group/project) |
| `CI_COMMIT_REF_NAME` | Current branch name |
| `CI_MERGE_REQUEST_IID` | MR number |
| `CI_MERGE_REQUEST_SOURCE_BRANCH_NAME` | MR source branch |
| `CI_MERGE_REQUEST_TARGET_BRANCH_NAME` | MR target branch |
| `CI_PIPELINE_ID` | Pipeline ID |

#### CircleCI Variables

| Variable | Description |
|----------|-------------|
| `CIRCLECI` | Detected automatically |
| `CIRCLE_PROJECT_USERNAME` | Project owner |
| `CIRCLE_PROJECT_REPONAME` | Repository name |
| `CIRCLE_BRANCH` | Current branch name |
| `CIRCLE_PULL_REQUEST` | Pull request URL (PR number extracted) |

### Concurrency Control

Use GitHub's concurrency feature to prevent race conditions:

```yaml
concurrency:
  group: parquedb-${{ github.event.pull_request.number }}
  cancel-in-progress: true
```

This ensures only one workflow runs per PR at a time.

### Required Permissions

For workflows using `actions/github-script`, ensure proper permissions:

```yaml
permissions:
  contents: read
  pull-requests: write
  checks: write
```

---

## Examples

### Complete CI/CD Pipeline

This example combines multiple features into a comprehensive pipeline:

```yaml
name: ParqueDB CI/CD

on:
  pull_request:
    types: [opened, synchronize, closed]
  push:
    branches: [main]

concurrency:
  group: parquedb-${{ github.event.pull_request.number || github.sha }}
  cancel-in-progress: true

permissions:
  contents: read
  pull-requests: write
  checks: write

jobs:
  # Create preview environment for PRs
  preview:
    name: Preview Environment
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request' && github.event.action != 'closed'
    steps:
      - uses: actions/checkout@v4
      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Create preview
        run: parquedb ci preview create --pr ${{ github.event.pull_request.number }}

  # Check for merge conflicts
  conflicts:
    name: Conflict Check
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request' && github.event.action != 'closed'
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Check conflicts
        run: parquedb ci check-merge --verbose

  # Generate and post diff
  diff:
    name: Database Diff
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request' && github.event.action != 'closed'
    steps:
      - uses: actions/checkout@v4
      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Generate diff
        id: diff
        run: |
          parquedb ci diff --format markdown > diff.md
          echo "diff<<EOF" >> $GITHUB_OUTPUT
          cat diff.md >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT

      - name: Post diff comment
        uses: actions/github-script@v7
        with:
          script: |
            const diff = `${{ steps.diff.outputs.diff }}`;
            await github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.payload.pull_request.number,
              body: `## Database Changes\n\n${diff}`
            });

  # Cleanup on PR close
  cleanup:
    name: Cleanup
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request' && github.event.action == 'closed'
    steps:
      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Delete preview
        run: parquedb ci preview delete --pr ${{ github.event.pull_request.number }}

  # Auto-merge on PR merge
  auto-merge:
    name: Auto Merge
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request' && github.event.action == 'closed' && github.event.pull_request.merged == true
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Merge and push
        run: |
          parquedb merge --branch preview-${{ github.event.pull_request.number }}
          parquedb push
```

### Matrix Testing

Test against multiple ParqueDB versions:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        parquedb-version: ['1.0.0', '1.1.0', 'latest']
    steps:
      - uses: actions/checkout@v4
      - uses: parquedb/setup-action@v1
        with:
          version: ${{ matrix.parquedb-version }}

      - name: Run tests
        run: npm test
```

### Manual Workflow Dispatch

Allow manual database operations:

```yaml
name: Database Operations

on:
  workflow_dispatch:
    inputs:
      operation:
        description: 'Operation to perform'
        required: true
        type: choice
        options:
          - diff
          - merge
          - push
      branch:
        description: 'Branch name'
        required: false
        default: 'main'

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: parquedb/setup-action@v1
        with:
          token: ${{ secrets.PARQUEDB_TOKEN }}

      - name: Run operation
        run: |
          case "${{ inputs.operation }}" in
            diff)
              parquedb diff ${{ inputs.branch }}
              ;;
            merge)
              parquedb merge ${{ inputs.branch }}
              ;;
            push)
              parquedb push
              ;;
          esac
```

### GitLab CI Pipeline

```yaml
stages:
  - preview
  - check

variables:
  PARQUEDB_TOKEN: $PARQUEDB_TOKEN

preview:
  stage: preview
  image: node:20
  before_script:
    - npm install -g parquedb
  script:
    - parquedb ci preview create
  rules:
    - if: $CI_MERGE_REQUEST_IID

diff:
  stage: check
  image: node:20
  before_script:
    - npm install -g parquedb
  script:
    - parquedb ci diff --format markdown
  rules:
    - if: $CI_MERGE_REQUEST_IID

merge-check:
  stage: check
  image: node:20
  before_script:
    - npm install -g parquedb
  script:
    - parquedb ci check-merge
  rules:
    - if: $CI_MERGE_REQUEST_IID
```

### CircleCI Pipeline

```yaml
version: 2.1

executors:
  node:
    docker:
      - image: cimg/node:20.0

jobs:
  database-preview:
    executor: node
    steps:
      - checkout
      - run:
          name: Install ParqueDB CLI
          command: npm install -g parquedb
      - run:
          name: Create preview
          command: parquedb ci preview create

  database-check:
    executor: node
    steps:
      - checkout
      - run:
          name: Install ParqueDB CLI
          command: npm install -g parquedb
      - run:
          name: Run diff
          command: parquedb ci diff --format markdown
      - run:
          name: Check merge
          command: parquedb ci check-merge

workflows:
  database:
    jobs:
      - database-preview
      - database-check
```

---

## Troubleshooting

### No CI environment detected

**Error:**
```
Error: No CI environment detected. Supported CI providers: GitHub Actions, GitLab CI, CircleCI
```

**Cause:** Running CI commands outside a supported CI environment.

**Solution:** For local testing, use explicit flags instead of auto-detection:

```bash
parquedb diff main feature/branch --base main --head feature/branch
```

---

### Invalid ParqueDB token

**Error:**
```
Error: Invalid ParqueDB token. Get a token from oauth.do
```

**Solutions:**

1. Get a new token from [oauth.do](https://oauth.do)
2. Verify the token is added as repository secret named `PARQUEDB_TOKEN`
3. Check the token is passed correctly:

```yaml
- uses: parquedb/setup-action@v1
  with:
    token: ${{ secrets.PARQUEDB_TOKEN }}
```

Or as environment variable:

```yaml
- name: Run command
  run: parquedb ci diff
  env:
    PARQUEDB_TOKEN: ${{ secrets.PARQUEDB_TOKEN }}
```

---

### PR number not found

**Error:**
```
Error: PR number not found in environment. Use --pr <number> or --branch <name>
```

**Cause:** CI environment variables for PR number not available.

**Solutions:**

1. Use explicit `--pr` flag:
   ```bash
   parquedb ci preview create --pr ${{ github.event.pull_request.number }}
   ```

2. Or use custom branch name:
   ```bash
   parquedb ci preview create --branch my-preview-branch
   ```

---

### Base and head branches not found

**Error:**
```
Error: Base and head branches not found. Use --base and --head flags or set CI environment variables
```

**Cause:** CI environment variables for branches not available.

**Solution:** Use explicit flags:

```bash
parquedb ci diff --base ${{ github.base_ref }} --head ${{ github.head_ref }}
```

---

### Preview branch already exists

**Message:**
```
Updating existing preview branch: pr-123
```

This is expected behavior. ParqueDB updates the existing preview branch on each push to the PR.

---

### Conflicts detected

**Error:**
```
Conflicts detected:
  - users/user-123
2 conflicts found
```

**Solutions:**

1. Resolve conflicts locally:
   ```bash
   parquedb conflicts
   parquedb resolve users/user-123 --ours
   parquedb merge --continue
   ```

2. Or use auto-resolution strategy:
   ```bash
   parquedb ci check-merge --strategy newest
   ```

---

### Timeout errors

**Solution:** Add timeout to long-running jobs:

```yaml
jobs:
  preview:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      # ...
```

---

### Permission denied errors

**Solution:** Add required permissions to your workflow:

```yaml
permissions:
  contents: read
  pull-requests: write
  checks: write
```

---

### Cache not working

**Symptoms:** The setup action downloads the CLI on every run.

**Solutions:**

1. Ensure consistent runner type (cache is per-runner)
2. Pin to a specific version:
   ```yaml
   - uses: parquedb/setup-action@v1
     with:
       version: '1.2.3'
   ```

---

## Security Considerations

1. **Token Storage** - Always store `PARQUEDB_TOKEN` as a repository secret, never in code
2. **Branch Protection** - Enable branch protection rules to enforce merge checks
3. **Unlisted Visibility** - Preview branches are created with unlisted visibility by default
4. **Concurrency Groups** - Workflow templates use concurrency groups to prevent race conditions
5. **Limited Permissions** - Only request the permissions your workflow needs

---

## Related Resources

- [Database Branching Quickstart](./guides/branching-quickstart.md) - Local branching workflow
- [Sync Guide](./SYNC.md) - Push/pull databases to remote storage
- [CLI Reference](./parquedb-api.md) - Full CLI documentation
- [Schema Evolution](./schema-evolution.md) - Schema migration strategies
