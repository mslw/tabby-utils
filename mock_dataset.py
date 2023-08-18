"""This script is for adding mock entries to a catalog to see what
the catalog software would make of them.

"""
import html
import json
from pathlib import Path
import tempfile

from datalad.api import Dataset, catalog_add, catalog_remove, catalog_set, catalog_validate
from datalad.support.exceptions import IncompleteResultsError

DSID = "1234"
DSVER = "latest"

# start with the basics
meta_item = {
    "type": "dataset",
    "dataset_id": DSID,
    "dataset_version": DSVER,
    "name": "Test dataset",
}

# add some more
# meta_item["license"] = {"url": "https://creativecommons.org/publicdomain/zero/1.0/"}
# meta_item["license"] = {"name": "https://creativecommons.org/publicdomain/zero/1.0/"}

meta_item["license"] = {"name": html.unescape("Creative Commons &mdash; CC0 1.0 Universal"),
                        "url": "https://creativecommons.org/publicdomain/zero/1.0/",
                        }

catalog_dir = Path("catalog")

# res = catalog_add(catalog=catalog_dir, metadata=json.dumps(meta_item))

# with tempfile.NamedTemporaryFile(mode="w+t") as f:
#     json.dump(meta_item, f)
#     f.seek(0)
#     res = catalog_add(catalog=catalog_dir, metadata=f.name)

# why catalog is needed: https://github.com/datalad/datalad-catalog/issues/330
catalog_validate(catalog=catalog_dir, metadata=json.dumps(meta_item))

try:
    catalog_remove(
        catalog=catalog_dir,
        dataset_id=DSID,
        dataset_version=DSVER,
        reckless=True,
        on_failure="continue",  # didn't work for some reason?
    )
except IncompleteResultsError:
    pass

catalog_add(catalog=catalog_dir, metadata=json.dumps(meta_item))

try:
    catalog_set(
        catalog=catalog_dir,
        property="home",
        dataset_id=DSID,
        dataset_version=DSVER,
        reckless="overwrite",
    )
except IncompleteResultsError:
    pass

# it turns out name is required...
"""
idea: urlparse
  - (is url?) check if scheme is (http, https) and netloc exists
  - either look up, or take without scheme
"""
