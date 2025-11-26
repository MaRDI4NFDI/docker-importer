from flask import Flask, request, jsonify 
from mardi_importer.wikidata import WikidataImporter
from mardi_importer import Importer
import re

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)

def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [v.strip() for v in re.split(r'[,\s]+', value) if v.strip()]
    return [str(value).strip()]

@app.post("/import/wikidata")
def import_wikidata():
    data = request.get_json(silent=True) or {}
    qids = as_list(data.get("qids"))
    if not qids:
        return jsonify(error="missing qids"), 400
    wdi = WikidataImporter()
    results, errors = [], []
    for q in qids:
        try:
            imported_q = wdi.import_entities(q)
            results.append({"qid": imported_q, "result": imported_q is not None})
        except Exception as e:
            log.error("importing wikidata failed: %s", e, exc_info=True)
            errors.append({"qid": q, "error": str(e)})
    return jsonify({
        "qids": qids,
        "count": len(qids),
        "results": results,
        "errors": errors,
        "imported": len(errors) == 0
    }), 200


@app.post("/import/doi")
def import_doi():
    data = request.get_json(silent=True) or {}
    dois = as_list(data.get("dois"))
    if not dois:
        return jsonify(error="missing doi"), 400
    dois = [x.upper() for x in dois]
    results, errors = [], []
    if any("ARXIV" in d for d in dois):
        arxiv = Importer.create_source('arxiv')
    if any("ZENODO" in d for d in dois):
        zenodo = Importer.create_source('zenodo')
    if not all(("ARXIV" in d) or ("ZENODO" in d)for d in dois):
        crossref = Importer.create_source('crossref')

    for doi in dois:
        try:
            if "ARXIV" in doi:
                arxiv_id = doi.split("/")[-1]
                publication = arxiv.new_publication(arxiv_id)
            elif "ZENODO" in doi:
                zenodo_id = doi.split(".")[-1]
                publication = zenodo.new_resource(zenodo_id)
            else:
                publication = crossref.new_publication(doi)
            result = publication.create()
            results.append({"doi":doi, "result": result})
        except Exception as e: 
            log.error("importing doi failed: %s", e, exc_info=True)
            errors.append({"doi": doi, "error": str(e)})
    return jsonify({
        "dois": dois,
        "count": len(dois),
        "results": results,
        "errors": errors,
        "imported": len(errors) == 0
    }), 200

if __name__ == "__main__":
    app.run(host = "0.0.0.0", port=8000)
