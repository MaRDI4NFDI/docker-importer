from habanero import Crossref


def get_tag(tag_name, namespace):
    """
    Returns a fully qualified tag name.
    @param namespace URL of a namespace|None (OAI_NS is default)
    """
    return "{{{}}}{}".format(namespace, tag_name)


def parse_doi_info(val, work_info):
    """
    input:
        val: tag
        work_info: dict with infomation from doi query response
    """
    # information about return fields can be found under https://api.crossref.org/swagger-ui/index.html#/Works/get_works
    if val == "author":
        # author and the familiy subfield are mandatory fields in crossref api
        # looks like: 'author': [{'given': 'Max', 'family': 'Mustermann', 'sequence': 'first', 'affiliation': []}]
        if "author" not in work_info:
            return None
        all_authors = ""
        first_name = ""
        family_name = ""
        author_list = []
        for author_dict in work_info["author"]:
            # family name not known: too little information
            if "family" not in author_dict:
                return None
            family_name = author_dict["family"]
            # family name not known; too little information
            if not family_name:
                return None
            if "given" in author_dict:
                first_name = author_dict["given"]
            # first name not necessarily needed
            if not first_name:
                author_list.append(family_name)
            else:
                author_list.append(family_name + ", " + first_name)

        return ";".join(author_list)
    elif val == "document_title":
        if "document_title" not in work_info:
            return None
        title_list = work_info["title"]
        if title_list:
            return ";".join(title_list)
        else:
            return None
    elif val == "publication_year":
        # date-parts is a mandaory field for published in crossref api
        # 'published': {'date-parts': [[2008]]}} this is not necessarily the year this was published in the journal, apparently...
        if "published" not in work_info:
            return None
        # this is either a year or None
        return work_info["published"]["date_parts"][0][0]
    elif val == "serial":
        if "reference" not in work_info:
            return None
        serials = []
        for serial_dict in work_info["reference"]:
            if "journal_title" in serial_dict:
                serials.append(serial_dict["journal-title"])
        # if no serials were found
        if not serials:
            return None
        # make list unique
        serials = list(set(serials))
        return ";".join(serials)

    elif val == "language":
        if "language" not in work_info:
            return None
        return work_info["language"]
    elif val == "keywords":
        if "subject" not in work_info:
            return None
        return ";".join(work_info["subject"])
