# Cloud Run Runbook

## Why this setup
- Keep cost predictable while we are still testing.
- Make production startup fail immediately if secrets are missing.
- Keep deployment manual first so no automatic builds or deploys surprise us.
- Treat production as the default mode. Only set `APP_ENV=development` locally if you intentionally want a relaxed local fallback.

## Default cost guards
- Region: `us-west1`
- Min instances: `0`
- Max instances: `1`
- CPU: `1`
- Memory: `1Gi`
- Concurrency: `10`
- Timeout: `900s`

Raise `max instances` only after real traffic proves we need it.

## One-time GCP setup
1. Authenticate locally: `gcloud auth login`
2. Set the project: `gcloud config set project YOUR_PROJECT_ID`
3. Enable APIs:
   - `gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com`
4. Create the Artifact Registry repo once:
   - `gcloud artifacts repositories create cloud-run-images --repository-format=docker --location=us-west1`
5. Create the runtime service account once:
   - `gcloud iam service-accounts create startup-agent-run --display-name="Startup Agent Cloud Run Runtime"`
6. Create the runtime secrets once:
   - `printf '%s' 'YOUR_GOOGLE_API_KEY' | gcloud secrets create GOOGLE_API_KEY --data-file=-`
   - `printf '%s' 'YOUR_DATABASE_URL' | gcloud secrets create SUPABASE_DB_URL --data-file=-`
   - `printf '%s' 'https://YOUR_PROJECT.supabase.co' | gcloud secrets create SUPABASE_URL --data-file=-`
   - `printf '%s' 'YOUR_SUPABASE_SERVICE_ROLE_KEY' | gcloud secrets create SUPABASE_SERVICE_ROLE_KEY --data-file=-`

If a secret already exists, add a new version instead:
- `printf '%s' 'NEW_VALUE' | gcloud secrets versions add SECRET_NAME --data-file=-`

## Manual first deploy
1. Build the image:
   - `gcloud builds submit --tag us-west1-docker.pkg.dev/YOUR_PROJECT_ID/cloud-run-images/startup-agent:manual-001`
2. Apply all migrations in order before the first deploy:
   - `for file in migrations/*.sql; do psql "$YOUR_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$file"; done`
3. Deploy manually:
   - `gcloud run deploy startup-agent --image us-west1-docker.pkg.dev/YOUR_PROJECT_ID/cloud-run-images/startup-agent:manual-001 --region us-west1 --platform managed --allow-unauthenticated --service-account startup-agent-run@YOUR_PROJECT_ID.iam.gserviceaccount.com --cpu 1 --memory 1Gi --concurrency 10 --timeout 900 --min-instances 0 --max-instances 1 --set-env-vars APP_ENV=production,TASK_MGMT_V1=true,TASK_TOOL_CALLING_V1=true,STRICT_STARTUP_VALIDATION=true,LOG_LEVEL=INFO --set-secrets GOOGLE_API_KEY=GOOGLE_API_KEY:latest,SUPABASE_URL=SUPABASE_URL:latest,SUPABASE_SERVICE_ROLE_KEY=SUPABASE_SERVICE_ROLE_KEY:latest`
4. Capture the service URL:
   - `gcloud run services describe startup-agent --region us-west1 --format='value(status.url)'`

## Smoke test checklist
- `GET /health` returns `status=ok`, `repository_mode=supabase`, and `strict_startup_validation=true`
- `POST /agent/bootstrap-device` works
- `POST /agent/onboarding/complete` works
- `POST /agent/task-management` works
- WebSocket `/agent/ws` can connect, stream, and reconnect
- Logs show structured JSON entries for startup, HTTP requests, WebSocket lifecycle, and failures

## Observability
- Quick logs:
  - `gcloud run services logs read startup-agent --region us-west1 --limit 100`
- Live tail:
  - `gcloud beta run services logs tail startup-agent --region us-west1`
- Service details:
  - `gcloud run services describe startup-agent --region us-west1`

## Git-based deploy later
Do not create the GitHub trigger until the manual deploy is stable.

When ready:
1. Connect the repo to Cloud Build.
2. Create a trigger on `main`.
3. Point the trigger at `cloudbuild.yaml` so it runs the migration before deploy.
4. Grant the trigger service account Secret Manager access to `SUPABASE_DB_URL`, because that migration runs inside Cloud Build.
5. Keep the same low-cost defaults until real traffic justifies changing them.

## Rollout
1. Deploy Cloud Run manually.
2. Point the app to the new Cloud Run URL.
3. Keep the Render service alive for 7 days as rollback insurance.
4. Only after that week, delete the Render web service.
