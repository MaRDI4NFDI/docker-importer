from flask import Flask, request, jsonify 
from mardi_importer.integrator import MardiIntegrator
app = Flask(__name__)

@app.get("/test")
def test():
    return{"status": "ok"}

@app.post("/import/wikidata")
def import_wikidata():
    data = request.get_json()
    qid = data.get("qid")
    if not qid:
        return jsonify(error="missing qid"), 400
    integrator = MardiIntegrator()
    integrator.import_entities(id_list = qid)
    result = {"qid": qid, "imported": True}
    return jsonify(result), 202

@app.get("/test/wikidata")
def test_import_wikidata():
    qid = request.args.get("qid")
    if not qid:
        return jsonify(error="missing qid"), 400
    integrator = MardiIntegrator()
    integrator.import_entities(id_list = qid)
    result = {"qid": qid, "imported": True}
    return jsonify(result), 202


@app.get("/haha")
def haha():
    return "hahaha"

if __name__ == "__main__":
    app.run(host = "0.0.0.0", port=8000)
