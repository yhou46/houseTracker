# AWS Deployment

Deploys HouseTracker services (spiders + data ingestion) to AWS ECS Fargate, running daily at 18:00 PT via EventBridge Scheduler.

## Architecture

```
EventBridge Scheduler (18:00 PT daily)
    └── ECS Fargate Task (single task, shared network)
            ├── redis (sidecar)
            ├── property-url-discovery-spider
            ├── property-crawler-spider
            |── property-scan-service
            └── property-data-ingestion-service
```

All containers share the same network namespace (localhost) within a task. Redis acts as a sidecar — no ElastiCache needed.

## Prerequisites

- AWS CLI configured with SSO profile
- Docker running
- `jq` and `envsubst` installed (`brew install jq gettext`)

```bash
aws sso login --profile <your-profile>
```

## One-time Setup

Run these scripts once to provision AWS infrastructure. All scripts are idempotent.

### Step 1 — IAM Roles

```bash
./setup_iam_roles.sh
```

Creates three roles:
- `housetracker-ecs-task-execution-role` — allows ECS to pull images from ECR and write to CloudWatch Logs
- `housetracker-ecs-task-role` — allows containers to access DynamoDB and S3
- `housetracker-eventbridge-ecs-role` — allows EventBridge Scheduler to trigger ECS tasks

### Step 2 — ECR Repositories

```bash
./create_registries.sh
```

Creates two ECR repositories:
- `housetracker/redfin-spider`
- `housetracker/data-ingestion-service`

### Step 3 — Networking

```bash
./setup_networking.sh
```

Uses the default VPC and its subnets. Creates a security group `housetracker-ecs-tasks-sg` with outbound internet access (required to crawl redfin.com). No inbound rules — tasks only make outbound requests.

### Step 4 — ECS Cluster

```bash
./setup_ecs_cluster.sh
```

Creates an empty Fargate cluster named `housetracker`. No cost until tasks run.

### Step 5 — CloudWatch Log Group

```bash
./setup_cloudwatch_logs.sh
```

Creates `/ecs/housetracker` log group with 30-day retention.

### Step 6 — Build, Push & Register Task Definition

```bash
./deploy.sh
```

Builds Docker images for `linux/arm64` (Fargate Graviton), pushes to ECR with a date-based tag (e.g. `20260322.0`), and registers a new ECS task definition revision tagged with the image version.

See [Deployment](#deployment) for options.

### Step 7 — EventBridge Scheduler

```bash
./setup_eventbridge_rule.sh
```

Creates an EventBridge Scheduler rule that triggers the ECS task daily at 18:00 PT (`America/Los_Angeles` timezone, DST-aware). Updates the schedule if it already exists.

---

## Deployment

Use `deploy.sh` whenever you push new code:

```bash
# Build, push, and register new task definition (most common)
./deploy.sh

# Push images only, skip task definition update
./deploy.sh --skip-task-def

# Use a specific image tag
./deploy.sh --tag 20260322.0

# Use a specific tag, skip task definition update
./deploy.sh --tag 20260322.0 --skip-task-def
```

Image tags follow the format `YYYYMMDD.<n>` (e.g. `20260322.0`, `20260322.1`). Each `deploy.sh` run auto-increments the counter for the current date.

After deploying, re-run `setup_eventbridge_rule.sh` if you want the scheduler to use the new task definition revision.

---

## Running a Task Manually

```bash
./run_task.sh
```

Starts the ECS task immediately using the latest registered task definition. Useful for testing before the scheduled run.

View logs after the task starts:
```
https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/%2Fecs%2Fhousetracker
```

---

## Configuration

| File | Purpose |
|------|---------|
| `config/ecs_task_definition.json` | ECS task definition template (containers, memory, env vars) |
| `config/ecs_run_task_config.json` | Config for `run_task.sh` (cluster, security group, task family) |
