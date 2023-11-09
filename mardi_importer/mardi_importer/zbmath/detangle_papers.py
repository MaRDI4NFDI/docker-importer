from mardi_importer.integrator import MardiIntegrator

api = MardiIntegrator()
with open(infile, 'r') as f:
    with open("small_file.txt", "a+") as outfile:
        for line in f:
            qid = line.split(",")[0].split('/')[0][:-1] #  <https://portal.mardi4nfdi.de/entity/Q1177759>
            count = int(line.split(",").strip())
            item = api.item.get(entity_id=qid)
            zbmath_ids = item.zbmath_ids
            for zb_id in zbmath_ids:
                line = find_line(zbmath_file, zbmath_id)
                outfile.write(line)


def find_line(file, zbmath_id):
    with open(file, 'r') as f:
        for line in f:
            if zbmath_id in line:
                return(line)
    sys.exit(f"Line not found for id {zbmath_id}!")


#IMPORTANT: FOR THIS RUN, CLAIMS NEED TO BE REPLACED; NOT APPENDED! CHANGE BACK AFTER!