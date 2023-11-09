from mardi_importer.integrator import MardiIntegrator

integrator = MardiIntegrator()
with open(infile, 'r') as f:
    for line in f:
        e_id = line.split(",")[0].split('/')[0][:-1] #  <https://portal.mardi4nfdi.de/entity/Q1177759>
        count = int(line.split(",").strip())
        item = self.api.item.new()
        # get item for this id!
        # get first zbmath id 
        # save all other zbmath  ids to structure
        # delete all claims in item
        # change to infos from first 
        # log that that one is done
        # upload the ones from the list
        # for each, log that they are done


