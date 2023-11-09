# read file that has original ids and zbmath ids --> write query to even get that!
# for each original id:
    # take first zbmath id and get information for that
    # get item and delete all claims and re-add only those that belong to the specific id; remember to write id in description
    # write down all other zbmath ids in a file to be processed later
# feed list of zbmath ids to be processed to normal script and rewrite that so it works? or also do this in this script, but that would be harder I think...
# although everything I need should already be there