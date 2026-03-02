# LHP Web App — Docker Quickstart

Run the LHP web application locally with a single command using Docker.

## Prerequisites

- **Docker** and **Docker Compose** installed and running
- A local **LHP project directory** containing an `lhp.yaml` file
  (e.g. one created with `lhp init my_project` or cloned from a sample repo)
- (Optional) A **Databricks Personal Access Token** for AI chat features

## Step 1: Clone the repository

```bash
git clone https://github.com/Mmodarre/Lakehouse_Plumber.git
cd Lakehouse_Plumber
```

## Step 2: Configure your environment

Copy the example environment file and edit it:

```bash
cp .env.example .env
```

Open `.env` and set the path to your LHP project:

```bash
# REQUIRED — absolute path to your local LHP project directory
LHP_PROJECT_PATH=/path/to/your/lhp-project
```

> **Don't have a project yet?** Clone the sample project to get started:
> ```bash
> git clone https://github.com/Mmodarre/acme_supermarkets_lhp_sample.git ~/my-lhp-project
> ```
> Then set `LHP_PROJECT_PATH=~/my-lhp-project` in your `.env`.

### AI chat (optional)

To enable the AI assistant, also set these in your `.env`:

```bash
ANTHROPIC_BASE_URL=https://<workspace>.azuredatabricks.net/serving-endpoints/anthropic
ANTHROPIC_AUTH_TOKEN=<your-databricks-personal-access-token>
ANTHROPIC_API_KEY=unused
```

> **Note:** `ANTHROPIC_API_KEY=unused` is a required placeholder — actual authentication
> uses the Bearer token from `ANTHROPIC_AUTH_TOKEN`. This requires a Databricks workspace
> with the Anthropic model serving endpoint configured.

## Step 3: Build and start

```bash
docker compose up --build
```

The first build takes 3–5 minutes (downloading base images, installing dependencies).
Subsequent starts reuse the cached image and take seconds.

You should see output like:

```
=== LHP Docker Container ===
  Project root : /project
  Dev mode     : true
  AI enabled   : true
  ...
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

## Step 4: Open the app

Navigate to **http://localhost:8000** in your browser.

You'll see the LHP dashboard with your project's pipelines, flowgroups, and
dependency graph. If AI is configured, click the chat icon in the top-right to
ask questions about your pipeline configuration.

## Stopping

Press `Ctrl+C` in the terminal, or from another terminal:

```bash
docker compose down
```

## Day-to-day usage

After the initial setup, you only need:

```bash
cd Lakehouse_Plumber
docker compose up
```

No `--build` needed unless you've pulled new changes to the Lakehouse Plumber
repository itself. Changes to your LHP **project** files are picked up automatically
since the project directory is mounted as a volume.

## Convenience script

A wrapper script is provided that validates your configuration before starting:

```bash
./scripts/docker_dev.sh            # start (foreground)
./scripts/docker_dev.sh --build    # rebuild and start
./scripts/docker_dev.sh --detach   # start in background
./scripts/docker_dev.sh down       # stop
./scripts/docker_dev.sh logs -f    # follow logs
```

## Using a different port

Set `LHP_PORT` in your `.env` file:

```bash
LHP_PORT=9000
```

Then access the app at `http://localhost:9000`.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `No lhp.yaml found at /project/lhp.yaml` | `LHP_PROJECT_PATH` is wrong or unset | Check the path in `.env` points to a directory containing `lhp.yaml` |
| `error while creating mount source path` | Docker VM can't access the path (common on macOS with protected folders like `~/Documents`) | Move or copy your project to a non-protected location like `~/projects/` |
| AI chat shows "Thinking..." then fails | Rate limit or bad credentials | Check container logs with `docker compose logs` — look for 401 or 429 errors |
| Port already in use | Another process on port 8000 | Set `LHP_PORT=9000` in `.env`, or stop the other process |
| Build fails pulling images | Not logged in to Docker | Run `docker login` first |
