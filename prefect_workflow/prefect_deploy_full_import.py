# What this script does:
#   1. Creates a global concurrency limit of 1 so at most one run of
#      this flow can be active at a time (= "don't start if the previous
#      run hasn't finished").
#   2. Deploys the flow from the Git repo (manual trigger only, no schedule).
#   3. Passes ``job_variables`` that tell the Prefect K8s worker to mount
#      the PVC ``importer-workflow-data`` into the job pod.
# ---------------------------------------------------------------------------

from prefect import flow
from prefect.client.orchestration import get_client

# ── 1.  Concurrency limit ──────────────────────────────────────────────────
#
# Prefect's *global* concurrency limits cap how many matching runs can be
# active at once.  We create one with limit=1 so a second run will wait
# (queue) until the first finishes.
#
# The limit is identified by a tag.  We add the same tag to the deployment
# so every run of this deployment inherits it.

CONCURRENCY_TAG   = "full-import-lock"
CONCURRENCY_LIMIT = 1


def ensure_concurrency_limit() -> None:
    """Create (or update) the concurrency limit idempotently."""
    import asyncio
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate

    async def _ensure():
        async with get_client() as client:
            try:
                existing = await client.read_global_concurrency_limit_by_name(
                    CONCURRENCY_TAG
                )
                if existing.limit != CONCURRENCY_LIMIT:
                    await client.update_global_concurrency_limit(
                        name=CONCURRENCY_TAG,
                        concurrency_limit=GlobalConcurrencyLimitCreate(
                            name=CONCURRENCY_TAG,
                            limit=CONCURRENCY_LIMIT,
                        ),
                    )
                    print(f"Updated concurrency limit '{CONCURRENCY_TAG}' → {CONCURRENCY_LIMIT}")
                else:
                    print(f"Concurrency limit '{CONCURRENCY_TAG}' already exists (limit={CONCURRENCY_LIMIT})")
            except Exception:
                await client.create_global_concurrency_limit(
                    GlobalConcurrencyLimitCreate(
                        name=CONCURRENCY_TAG,
                        limit=CONCURRENCY_LIMIT,
                    )
                )
                print(f"Created concurrency limit '{CONCURRENCY_TAG}' (limit={CONCURRENCY_LIMIT})")

    asyncio.run(_ensure())


# ── 2.  Deploy ─────────────────────────────────────────────────────────────

# K8s job customisation – the interesting part is the PVC mount.
# The worker merges these into the Kubernetes Job manifest it creates.
JOB_VARIABLES = {
    "image": "ghcr.io/mardi4nfdi/docker-importer:main",

    # Volumes + mounts for the shared persistent volume.
    # The PVC "importer-workflow-data" must already exist in the namespace
    # (see k8s/pvc-workflow-data.yaml).
    "customizations": [
        {
            # Add the volume definition to the pod spec
            "op": "add",
            "path": "/spec/template/spec/volumes/-",
            "value": {
                "name": "workflow-data",
                "persistentVolumeClaim": {
                    "claimName": "importer-workflow-data",
                },
            },
        },
        {
            # Mount it into the container
            "op": "add",
            "path": "/spec/template/spec/containers/0/volumeMounts/-",
            "value": {
                "name": "workflow-data",
                "mountPath": "/mnt/workflow-data",
            },
        },
        {
            # Set the env var so the flow code knows where to find it
            "op": "add",
            "path": "/spec/template/spec/containers/0/env/-",
            "value": {
                "name": "CHECKPOINT_DIR",
                "value": "/mnt/workflow-data",
            },
        },
    ],
}


if __name__ == "__main__":
    # Ensure the concurrency limit exists before deploying.
    ensure_concurrency_limit()

    flow.from_source(
        source="https://github.com/MaRDI4NFDI/docker-importer.git",
        entrypoint="prefect_workflow/prefect_full_import.py:full_import_flow",
    ).deploy(
        name="prefect-full-import",
        work_pool_name="K8WorkerPool",
        tags=[CONCURRENCY_TAG],
        parameters={},
        job_variables=JOB_VARIABLES,
        # To add a weekly schedule later, uncomment:
        # cron="0 2 * * 0",
    )

    print("\n✔  Deployment 'full-import/prefect-full-import' created.")
    print("   Schedule : manual (no cron)")
    print("   Concurrency : 1 (via tag '%s')" % CONCURRENCY_TAG)
    print("   PVC      : importer-workflow-data → /mnt/workflow-data")