# Run this for K8 execution:
#   export PREFECT_API_URL=http://prefect-mardi.zib.de/api
#   export PREFECT_API_AUTH_STRING=admin:xxx
#
# To add a schedule:
#   * Go to "Deployments"
#   * Click on the deployment "prefect-mardi-importer"
#   * Click on "+ Schedule" (top right corner)
#
# Run this for LOCAL execution:
#   prefect config unset PREFECT_API_URL
#   prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
#   prefect server start

from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/MaRDI4NFDI/docker-importer.git",
        entrypoint="prefect_workflow/prefect_mardi_importer.py:prefect_mardi_importer_flow",
    ).deploy(
        # This must match Flask:
        #   workflow_name = "mardi-importer/prefect-mardi-importer"
        name="prefect-mardi-importer",
        work_pool_name="K8WorkerPool",
        parameters={
            # Must match flow signature
            "action": "not_set",
            # Default value; Flask will override on trigger
            "qids": [],
        },
        job_variables={
            "image": "ghcr.io/mardi4nfdi/docker-importer:main",
        },
    )
