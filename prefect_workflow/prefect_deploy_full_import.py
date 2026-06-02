# What this script does:
#   1. Deploys the flow from the Git repo (manual trigger only, no schedule).
#   2. Limits the deployment to one active run at a time, so a second run
#      will queue (wait) until the first finishes.
#   3. Passes ``job_variables`` that tell the Prefect K8s worker to mount
#      the PVC ``importer-workflow-data`` into the job pod.
# ---------------------------------------------------------------------------

from prefect import flow


# ── Deploy ─────────────────────────────────────────────────────────────────
#
# Single-run behaviour is enforced by the per-deployment ``concurrency_limit``
# below. In Prefect 3 a deployment concurrency limit caps how many runs of
# *this* deployment can be active at once; with the default ENQUEUE collision
# strategy, runs over the limit wait in "AwaitingConcurrencySlot" until a slot
# frees up. limit=1 therefore means "don't start if the previous run hasn't
# finished".

# K8s job customisation – the interesting part is the PVC mount.
# The worker merges these into the Kubernetes Job manifest it creates.
JOB_VARIABLES = {
    "image": "ghcr.io/mardi4nfdi/docker-importer:main",
    "volumes": [{
        "name": "workflow-data",
        "persistentVolumeClaim": {
            "claimName": "importer-workflow-data",
        },
    }],
    "volume_mounts": [{
        "name": "workflow-data",
        "mountPath": "/mnt/workflow-data",
    }],
    "env": {
        "PREFECT_LOGGING_LEVEL": "DEBUG",
        "PREFECT_LOGGING_INTERNAL_LEVEL": "DEBUG",
        "CHECKPOINT_DIR": "/mnt/workflow-data",
        "MEDIAWIKI_API_URL":   "http://wikibase/w/api.php",
        "WIKIBASE_URL":        "http://wikibase",
        "SPARQL_ENDPOINT_URL": "http://wdqs:9999/bigdata/namespace/wdq/sparql",
        "IMPORTER_API_URL":    "http://importer-api",
    },
}


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/MaRDI4NFDI/docker-importer.git",
        entrypoint="prefect_workflow/prefect_full_import.py:full_import_flow",
    ).deploy(
        name="prefect-full-import",
        work_pool_name="K8WorkerPool",
        concurrency_limit=1,          # one active run at a time; extra runs queue
        parameters={},
        job_variables=JOB_VARIABLES,
        # To add a weekly schedule later, uncomment:
        # cron="0 2 * * 0",
    )

    print("\n✔  Deployment 'full-import/prefect-full-import' created.")
    print("   Schedule    : manual (no cron)")
    print("   Concurrency : 1 (per-deployment limit, ENQUEUE)")
    print("   PVC         : importer-workflow-data → /mnt/workflow-data")