"""Check release metadata when it is included in the source checkout."""

import sys
import unittest
from pathlib import Path

from scripts.tools.check_catalog_workflow_drift import check_drift


class CatalogWorkflowDriftTest(unittest.TestCase):
    catalog_path = None

    def test_release_workflows_stay_synchronized(self):
        if self.catalog_path is None or not self.catalog_path.exists():
            self.skipTest("catalog-info.yaml is not included in this source checkout")
        self.assertTrue(check_drift(self.catalog_path))


if __name__ == "__main__":
    catalog = sys.argv.pop(1) if len(sys.argv) > 1 else ""
    CatalogWorkflowDriftTest.catalog_path = Path(catalog) if catalog else None
    unittest.main()
