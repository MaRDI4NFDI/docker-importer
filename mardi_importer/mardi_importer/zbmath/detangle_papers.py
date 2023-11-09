from mardi_importer.integrator import MardiIntegrator

def find_line(zbmath_id):
    with open("/data/zbmath_data_dump20230621-172025.csv", 'r') as f:
        for line in f:
            if zbmath_id in line:
                return(line)
    sys.exit(f"Line not found for id {zbmath_id}!")


api = MardiIntegrator()
with open("/data/test.csv", 'r') as f:
    with open("/data/small_file.csv", "a+") as outfile:
        for line in f:
            zbmath_ids = []
            qid = line.split(",")[0].split('/')[-1][:-1] #  <https://portal.mardi4nfdi.de/entity/Q1177759>
            count = int(line.split(",")[1].strip())
            item = api.item.get(entity_id=qid)
            claims = item.claims.get('BLAAAA') #change!!
            for c in claims: 
                zbmath_ids.append(c.mainsnak.datavalue["value"])
            for zb_id in zbmath_ids:
                line = find_line(zb_id)
                outfile.write(line)
#IMPORTANT: FOR THIS RUN, CLAIMS NEED TO BE REPLACED; NOT APPENDED! CHANGE BACK AFTER!
