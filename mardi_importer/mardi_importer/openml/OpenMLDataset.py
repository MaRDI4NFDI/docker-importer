import re
import sys
from .OpenMLPublication import OpenMLPublication
import validators

semantic_tags = [
"Agriculture",
"Astronomy",
"Chemistry",
"Computational Universe",
"Computer Systems",
"Culture",
"Demographics",
"Earth Science",
"Economics",
"Education",
"Geography",
"Government",
"Health",
"History",
"Human Activities",
"Images",
"Language",
"Life Science",
"Machine Learning",
"Manufacturing",
"Mathematics",
"Medicine",
"Meteorology",
"Physical Sciences",
"Politics",
"Social Media",
"Sociology",
"Statistics",
"Text & Literature",
"Transportation"]

class OpenMLDataset:
    def __init__(
            self,
            integrator, 
            name,
            dataset_id,
            version,
            creators,
            contributors,
            collection_date,
            upload_date,
            license,
            url,
            default_target_attribute,
            row_id_attribute,
            tags,
            original_data_url,
            paper_url,
            md5_checksum,
            features,
            num_binary_features,
            num_classes,
            num_features,
            num_instances,
            num_instances_missing_vals,
            num_missing_vals,
            num_numeric_features,
            num_symbolic_features,
            format
            ):
        self.api = integrator
        self.name = name #done
        self.dataset_id = str(dataset_id) #done
        self.version = version #done
        self.creators = creators
        self.contributors = contributors
        self.collection_date = collection_date
        self.upload_date = upload_date
        self.license = license
        self.url = url
        self.default_target_attribute = default_target_attribute
        self.row_id_attribute = row_id_attribute
        self.tags = tags
        self.original_data_url = original_data_url
        self.paper_url = paper_url
        self.md5_checksum = md5_checksum
        self.features = features
        self.num_binary_features = num_binary_features
        self.num_classes = num_classes
        self.num_features = num_features
        self.num_instances = num_instances
        self.num_instances_missing_vals = num_instances_missing_vals
        self.num_missing_vals = num_missing_vals
        self.num_numeric_features = num_numeric_features
        self.num_symbolic_features = num_symbolic_features
        self.format = format
        self.QID = None
        self.item = self.init_item()

    def init_item(self):
        item = self.api.item.new()
        item.labels.set(language="en", value=self.name)
        item.descriptions.set(language="en", value=f"OpenML dataset with id {self.dataset_id}")
        return(item)
    
    def create(self):
        self.item.add_claim("wdt:P31", "wd:Q1172284")
        self.insert_claims()
        dataset_id = self.item.write().id
        print(f"Dataset with id {dataset_id} created")
        return(dataset_id)
    
    def insert_claims(self):
        prop_nr = self.api.get_local_id_by_label("OpenML dataset ID", "property")
        self.item.add_claim(prop_nr, self.dataset_id)
        if self.version is not None and self.version != "None":
            prop_nr = self.api.get_local_id_by_label("dataset version", "property")
            self.item.add_claim(prop_nr, str(self.version))
        if self.creators and self.creators != "None":
            creator_claims = []
            prop_nr = self.api.get_local_id_by_label("author name string", "property")
            if not isinstance(self.creators, list):
                self.creators = [self.creators]
            for c in self.creators:
                claim = self.api.get_claim(prop_nr, c)
                creator_claims.append(claim)
            self.item.add_claims(creator_claims)
        if self.contributors and self.contributors != "None":
            contributor_claims = []
            prop_nr = self.api.get_local_id_by_label("author name string", "property")
            if not isinstance(self.contributors, list):
                self.contributors = [self.contributors]
            for c in self.contributors:
                claim = self.api.get_claim(prop_nr, c)
                contributor_claims.append(claim)
            self.item.add_claims(contributor_claims)
        if self.collection_date and self.collection_date != "None":
            prop_nr = self.api.get_local_id_by_label("collection date", "property")
            self.item.add_claim(prop_nr, str(self.collection_date))
        if self.upload_date and self.upload_date != "None":
            prop_nr = self.api.get_local_id_by_label("upload date", "property")
            date = self.upload_date.split("T")[0] + "T00:00:00Z"
            self.item.add_claim(prop_nr, date)
        if self.license and self.license != "None":
            claims = self.process_licenses()
            self.item.add_claims(claims)
        url_claims = []
        if self.url and self.url != "None" and validators.url(self.url):
            claim = self.api.get_claim("wdt:P953", self.url)
            url_claims.append(claim)
        if self.original_data_url and self.original_data_url != "None" and validators.url(self.original_data_url):
            claim = self.api.get_claim("wdt:P953", self.original_data_url)
            url_claims.append(claim)
        if url_claims:
            self.item.add_claims(url_claims)
        if self.default_target_attribute and self.default_target_attribute != "None":
            prop_nr = self.api.get_local_id_by_label("default target attribute", "property")
            self.item.add_claim(prop_nr, self.default_target_attribute)
        if self.row_id_attribute and self.row_id_attribute != "None":
            prop_nr = self.api.get_local_id_by_label("row id attribute", "property")
            self.item.add_claim(prop_nr, self.row_id_attribute)
        if self.tags and self.tags != "None":
            valid_tags = []
            for t in self.tags:
                if t in semantic_tags:
                    valid_tags.append(t)
            if valid_tags:
                prop_nr = self.api.get_local_id_by_label("OpenML semantic tag", "property")
                tag_claims = []
                for vt in valid_tags:
                    claim = self.api.get_claim(prop_nr, vt)
                    tag_claims.append(claim)
                self.item.add_claims(tag_claims)
        if self.paper_url and self.paper_url != "None":
            #create item for this
            identifier, identifier_type = self.get_identifier()
            if identifier:
                publication = OpenMLPublication(integrator=self.api, identifier=identifier, 
                    identifier_type=identifier_type)
                paper_qid = publication.exists()
                if not paper_qid:
                    paper_qid = publication.create()
                self.item.add_claim("wdt:P2860", paper_qid)
            #create item for string
            prop_nr = self.api.get_local_id_by_label("citation text", "property")
            self.item.add_claim(prop_nr, self.paper_url)
        if self.md5_checksum and self.md5_checksum != "None":
            qualifier = [self.api.get_claim("wdt:P459", "wd:Q185235")]
            self.item.add_claims(self.api.get_claim("wdt:P4092", self.md5_checksum, qualifiers=qualifier))
        if self.features and self.features != "None":
            for _, v in self.features.items():
                full_feature = str(v).split(" - ")[1][:-1]
                match = re.match(r'^(.*?)\s*\(([^()]+)\)$', full_feature)
                if match:
                    feature = match.group(1).strip()
                    feature_type = match.group(2).strip()
                    if feature_type not in ["numeric", "nominal", "string", "date"]:
                        sys.exit("Incorrect feature type {feature_type}")
                    data_type_prop_nr = self.api.get_local_id_by_label("data type", "property")
                    qualifier = [self.api.get_claim(data_type_prop_nr, feature_type)]
                    feature_prop_nr = self.api.get_local_id_by_label("has feature", "property")
                    self.item.add_claims(self.api.get_claim(feature_prop_nr, feature, qualifiers=qualifier))
        if self.num_binary_features is not None and self.num_binary_features != "None":
            prop_nr = self.api.get_local_id_by_label("number of binary features", "property")
            self.item.add_claim(prop_nr, int(self.num_binary_features))
        if self.num_classes is not None and self.num_classes != "None":
            prop_nr = self.api.get_local_id_by_label("number of classes", "property")
            self.item.add_claim(prop_nr, int(self.num_classes))
        if self.num_features is not None and self.num_features != "None":
            prop_nr = self.api.get_local_id_by_label("number of features", "property")
            self.item.add_claim(prop_nr, int(self.num_features))
        if self.num_instances is not None and self.num_instances != "None":
            prop_nr = self.api.get_local_id_by_label("number of instances", "property")
            self.item.add_claim(prop_nr, int(self.num_instances))
        if self.num_instances_missing_vals is not None and self.num_instances_missing_vals != "None":
            prop_nr = self.api.get_local_id_by_label("number of instances with missing values", "property")
            self.item.add_claim(prop_nr, int(self.num_instances_missing_vals))
        if self.num_missing_vals is not None and self.num_missing_vals != "None":
            prop_nr = self.api.get_local_id_by_label("number of missing values", "property")
            self.item.add_claim(prop_nr, int(self.num_missing_vals))
        if self.num_numeric_features is not None and self.num_numeric_features != "None":
            prop_nr = self.api.get_local_id_by_label("number of numeric features", "property")
            self.item.add_claim(prop_nr, int(self.num_numeric_features))
        if self.num_symbolic_features is not None and self.num_symbolic_features != "None":
            prop_nr = self.api.get_local_id_by_label("number of symbolic features", "property")
            self.item.add_claim(prop_nr, int(self.num_symbolic_features))
        if self.format and self.format != "None":
            if self.format.lower() == "arff":
                self.item.add_claim("wdt:P2701", "wd:Q4489412")
            elif self.format.lower() == "sparse_arff":
                qid = self.api.get_local_id_by_label("Sparse ARFF", "item")
                self.item.add_claim("wdt:P2701", qid)
            else:
                sys.exit(f"Invalid file format {self.format}")
        profile_prop = self.api.get_local_id_by_label("MaRDI profile type", "property")
        profile_target = self.api.get_local_id_by_label("MaRDI dataset profile", "property")
        self.item.add_claim(profile_prop, profile_target)

    def exists(self):
        """Checks if a WB item corresponding to the dataset already exists.
        Searches for a WB item with the package label in the SQL Wikibase
        tables and returns **True** if a matching result is found.
        It uses for that the :meth:`mardi_importer.wikibase.WBItem.instance_exists()`
        method.
        Returns:
          String: Entity ID
        """
        if self.QID:
            return self.QID
        # instance of scholarly article
        self.QID = self.item.is_instance_of_with_property(
                "wd:Q1172284", "wdt:P11238", self.dataset_id
            )
        if self.QID:
            print(f"Dataset exists with QID {self.QID}")
        return self.QID

    def update(self):
        """
        Update existing item.
        """
        self.item = self.api.item.get(entity_id=self.QID)

        self.insert_claims()
        self.item.write()

        if self.QID:
            print(f"Dataset with ID {self.QID} has been updated.")
            return self.QID
        else:
            print(f"Dataset could not be updated.")
            return None


    def process_licenses(self):
        """Processes the license string and adds the corresponding statements.

        The concrete License is identified and linked to the corresponding
        item that has previously been imported from Wikidata. Further license
        information, when provided between round or square brackets, is added
        as a qualifier.

        If a file license is mentioned, the linked to the file license
        in CRAN is added as a qualifier.

        Args:
          item (WBItem):
            Item representing the R package to which the statement must be added.
        """
        license_str = self.license
        claims = []
        license_qualifier = ""
        if re.findall("\(.*?\)", license_str):
            qualifier_groups = re.search("\((.*?)\)", license_str)
            license_qualifier = qualifier_groups.group(1)
            license_aux = re.sub("\(.*?\)", "", license_str)
            if re.findall("\[.*?\]", license_aux):
                qualifier_groups = re.search("\[(.*?)\]", license_str)
                license_qualifier = qualifier_groups.group(1)
                license_str = re.sub("\[.*?\]", "", license_aux)
            else:
                license_str = license_aux
        elif re.findall("\[.*?\]", license_str):
            qualifier_groups = re.search("\[(.*?)\]", license_str)
            license_qualifier = qualifier_groups.group(1)
            license_str = re.sub("\[.*?\]", "", license_str)
        license_str = license_str.strip()
        license_QID = self.get_license_QID(license_str)
        if license_QID:
            if license_qualifier:
                qualifier = [self.api.get_claim("wdt:P9767", license_qualifier)]
                claims.append(self.api.get_claim("wdt:P275", license_QID, qualifiers=qualifier))
            else:
                claims.append(self.api.get_claim("wdt:P275", license_QID))
        return claims

    def get_identifier(self):
        if self.paper_url is None or self.paper_url == "None":
            return(None, None)
        elif "http" not in self.paper_url:
            return(None,None)
        elif "dl.acm.org" in self.paper_url:
            return("/".join(self.paper_url.split("/")[-2:]).lower(), "doi")
        elif "doi=" in self.paper_url:
            doi = self.paper_url.split("doi=")[-1]
            if "&" in doi:
                doi = doi.split("&")[0]
            return(doi.lower(), "doi")
        elif "link.springer" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            if "%" in doi: 
                return(None, None)
            else:
                return(doi.lower(), "doi")
        elif "wiley" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            if "?" in doi:
                doi = doi.split("?")[0]
            return(doi.lower(), "doi")
        elif "biomedcentral" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            return(doi.lower(), "doi")
        elif "tandfonline" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            return(doi.lower(), "doi")
        elif "arxiv" in self.paper_url:
            arxiv_id = self.paper_url.split("/")[-1]
            return(arxiv_id, "arxiv")
        elif "royalsociety" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            return(doi.lower(), "doi")
        elif "sagepub" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            return(doi.lower(), "doi")
        elif "science.org" in self.paper_url:
            doi = "/".join(self.paper_url.split("/")[-2:])
            return(doi.lower(), "doi")
        else:
            return(None, None)
    
    def get_license_QID(self, license_str: str) -> str:
        """Returns the Wikidata item ID corresponding to a software license.

        The same license is often denominated in CRAN using differents names.
        This function returns the wikidata item ID corresponding to a single
        unique license that is referenced in CRAN under different names (e.g.
        *Artistic-2.0* and *Artistic License 2.0* both refer to the same
        license, corresponding to item *Q14624826*).

        Args:
            license_str (str): String corresponding to a license imported from CRAN.

        Returns:
            (str): Wikidata item ID.
        """
        def get_license(label: str) -> str:
            license_item = self.api.item.new()
            license_item.labels.set(language="en", value=label)
            return license_item.is_instance_of("wd:Q207621")

        license_mapping = {
            "ACM": get_license("ACM Software License Agreement"),
            "AGPL":"wd:Q28130012",
            "AGPL-3": "wd:Q27017232",
            "Apache License": "wd:Q616526",
            "Apache License 2.0": "wd:Q13785927",
            "Apache License version 1.1": "wd:Q17817999",
            "Apache License version 2.0": "wd:Q13785927",
            "Artistic-2.0": "wd:Q14624826",
            "Artistic License 2.0": "wd:Q14624826",
            "BSD 2-clause License": "wd:Q18517294",
            "BSD 3-clause License": "wd:Q18491847",
            "BSD_2_clause": "wd:Q18517294",
            "BSD_3_clause": "wd:Q18491847",
            "BSL": "wd:Q2353141",
            "BSL-1.0": "wd:Q2353141",
            "CC0": "wd:Q6938433",
            "CC BY 4.0": "wd:Q20007257",
            "CC BY-SA 4.0": "wd:Q18199165",
            "CC BY-NC 4.0": "wd:Q34179348",
            "CC BY-NC-SA 4.0": "wd:Q42553662",
            "CeCILL": "wd:Q1052189",
            "CeCILL-2": "wd:Q19216649",
            "Common Public License Version 1.0": "wd:Q2477807",
            "CPL-1.0": "wd:Q2477807",
            "Creative Commons Attribution 4.0 International License": "wd:Q20007257",
            "EPL": "wd:Q1281977",
            "EUPL": "wd:Q1376919",
            "EUPL-1.1": "wd:Q1376919",
            "file LICENCE": get_license("File License"),
            "file LICENSE": get_license("File License"),
            "FreeBSD": "wd:Q34236",
            "GNU Affero General Public License": "wd:Q1131681",
            "GNU General Public License": "wd:Q7603",
            "GNU General Public License version 2": "wd:Q10513450",
            "GNU General Public License version 3": "wd:Q10513445",
            "GPL": "wd:Q7603",
            "GPL-2": "wd:Q10513450",
            "GPL-3": "wd:Q10513445",
            "LGPL": "wd:Q192897",
            "LGPL-2": "wd:Q23035974",
            "LGPL-2.1": "wd:Q18534390",
            "LGPL-3": "wd:Q18534393",
            "Lucent Public License": "wd:Q6696468",
            "MIT": "wd:Q334661",
            "MIT License": "wd:Q334661",
            "Mozilla Public License 1.1": "wd:Q26737735",
            "Mozilla Public License 2.0": "wd:Q25428413",
            "Mozilla Public License Version 2.0": "wd:Q25428413",
            "MPL": "wd:Q308915",
            "MPL version 1.0": "wd:Q26737738",
            "MPL version 1.1": "wd:Q26737735",
            "MPL version 2.0": "wd:Q25428413",
            "MPL-1.1": "wd:Q26737735",
            "MPL-2.0": "wd:Q25428413",
            "Unlimited": get_license("Unlimited License"),
        }

        license_info = license_mapping.get(license_str)
        if callable(license_info):
            return license_info()
        else:
            return license_info