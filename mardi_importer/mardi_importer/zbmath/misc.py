from habanero import Crossref
from requests.exceptions import HTTPError
import requests


def search_item_by_property(property_id,value):
    """
    Search for pages in namespace 120 that have the statement:
        haswbstatement:P<property_id>=<value>
    
    :param value: e.g. "6369674"
    :return: JSON response from the API
    """
    base_url = "https://portal.mardi4nfdi.de/w/api.php"
    # Create the search query with the property_id and value
    srsearch_query = f"haswbstatement:{property_id}={value}"
    
    # Set up the parameters for the API request
    params = {
        "action": "query",
        "list": "search",
        "srsearch": srsearch_query,
        "srnamespace": "120",  # Adjust if needed
        "format": "json"
    }
    
    response = requests.get(base_url, params=params)
    # Raise an exception if the request was unsuccessful
    response.raise_for_status()
    
    # Parse the response as JSON
    data = response.json()
    if data['query']['search']:
        qid = data['query']['search'][0]['title'].split(':')[-1]
    else:
        qid = None
    return qid


def get_tag(tag_name, namespace):
    """
    Returns a fully qualified tag name.

    Args:
        tag_name (string): name of tag, e.g. author
        namespace (string): namespace URL of a namespace
    """
    return "{{{}}}{}".format(namespace, tag_name)


def parse_doi_info(val, work_info):
    """
    Function to extract information returned by a doi query for a specific tag.

    Args:
        val (string): tag, e.g. author
        work_info (dict): information from doi query response

    Returns:
        string: information for specific tag, None if not found
    """
    # information about return fields can be found under https://api.crossref.org/swagger-ui/index.html#/Works/get_works
    if val == "author":
        # author and the familiy subfield are mandatory fields in crossref api
        # looks like: 'author': [{'given': 'Max', 'family': 'Mustermann', 'sequence': 'first', 'affiliation': []}]
        if "author" not in work_info:
            return None
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


def get_info_from_doi(doi, key):
    """
    Query crossref API for DOI information.

    Args:
        doi: doi
        key: document_title only for now

    Returns:
        title: document title
    """
    doi_list = doi.split(";")
    # print("doi")
    # print(doi)
    # print("doi list")
    # print(doi_list)
    cr = Crossref(mailto="pusch@zib.de")
    for doi in doi_list:
        try:
            work_info = cr.works(ids=doi)
            if not work_info:
                continue
            if key == "document_title":
                if "title" not in work_info["message"]:
                    continue
                title_list = work_info["message"]["title"]
                if title_list:
                    joint_title = ";".join(title_list).strip()
                    joint_title = joint_title.replace("\n", " ").strip()
                    joint_title = joint_title.replace("\t", " ").strip()
                    if len(joint_title) > 500:
                        return None
                    return joint_title
                else:
                    continue
            elif key == "journal":
                if "container-title" not in work_info["message"]:
                    return None
                if not work_info["message"]["container-title"]:
                    return None 
                journal = work_info["message"]["container-title"][0].strip()
                return journal
                # if the doi is not found, there is a 404
        except HTTPError:
            print("HTTP Error!")
            continue
    return None
