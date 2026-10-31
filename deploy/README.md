# Deployment

Merging to `main` deploys whatever changed. `.github/workflows/deploy.yml` runs CI again, works out the affected functions from [`lambdas.json`](lambdas.json), and updates their code. It only changes code: env vars, IAM and schedules stay as they are.

| Changed | Deployed |
|---|---|
| A Lambda script listed in `lambdas.json` | That function's zip |
| `dubizzle_dbt/`, `Lambda Scripts/dbt-runner/` or `.dockerignore` | A new `DbtRunner` image |
| Anything else | Nothing |

**Daily run window:** deploys fail on purpose between 03:25 and 04:20 UTC, so the Lambda chain never runs on mixed versions. Re-run the workflow afterwards. The window is `deploy_block_utc` in `lambdas.json`; change it if the EventBridge schedule moves.

**Manual deploy:** Actions → Deploy → Run workflow, and pick a function. This only works from `main`.

**Rollback:** `git revert` the change and merge it. Functions aren't invoked after a deploy because that would run the real pipeline, so the next daily digest is the end-to-end check. If a bad merge damages Gold, restore `cars_scd.parquet` from an earlier S3 version.

## Check for drift
Read-only. It compares the live code with the repo and flags any file a deploy would drop:

```bash
python scripts/deploy_lambdas.py check --profile YOUR_PROFILE
```

## One-time AWS setup
Run these once with an administrator (not root) profile, replacing `ACCOUNT_ID` in `aws/*.json` first without committing it. GitHub gets short-lived credentials through OIDC, so no AWS keys are stored in the repo. The role can only be assumed from the `production` GitHub environment, which is limited to `main`.

The `sub` value in `trust-policy.json` must exactly match what GitHub sends, or the deploy fails with `Not authorized to perform sts:AssumeRoleWithWebIdentity`. This repo uses GitHub's immutable format, which includes the owner and repo IDs. In a fork or a copy of this project, use the `sub_claim_prefix` from this command instead, followed by `:environment:production`:

```bash
gh api repos/OWNER/REPO/actions/oidc/customization/sub
```

```bash
aws iam create-open-id-connect-provider --url https://token.actions.githubusercontent.com --client-id-list sts.amazonaws.com
```

```bash
aws iam create-role --role-name github-actions-deploy --assume-role-policy-document file://deploy/aws/trust-policy.json
```

```bash
aws iam put-role-policy --role-name github-actions-deploy --policy-name deploy --policy-document file://deploy/aws/deploy-policy.json
```

```bash
aws ecr put-lifecycle-policy --region eu-north-1 --repository-name dubizzle-dbt-runner --lifecycle-policy-text file://deploy/aws/ecr-lifecycle.json
```

The Gold bucket already has versioning on. This stops old versions of the daily file piling up forever by expiring them after 30 days:

```bash
aws s3api put-bucket-lifecycle-configuration --region eu-north-1 --bucket dubizzle-gold --lifecycle-configuration file://deploy/aws/gold-lifecycle.json
```

## One-time GitHub setup
1. Settings → Environments → New environment `production`. Under deployment branches, allow only `main`.
2. In that environment, add the secret `AWS_DEPLOY_ROLE_ARN` with the role's ARN.
3. Settings → Actions → General: require approval for workflows from all outside collaborators.
